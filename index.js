const express = require('express');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const AMO_DOMAIN = process.env.AMO_DOMAIN; // например: yourcompany.amocrm.ru
const AMO_TOKEN = process.env.AMO_TOKEN;   // долгосрочный токен
const PIPELINE_NAME = 'Новостройки 2.0';
const STAGE_NAME = 'Недозвон';
const GROUP_NAME = 'Клуб чемпионов';

// ─── AMO API HELPER ───────────────────────────────────────────────────────────
const amo = axios.create({
  baseURL: `https://${AMO_DOMAIN}/api/v4`,
  headers: { Authorization: `Bearer ${AMO_TOKEN}` }
});

// ─── UTILS ────────────────────────────────────────────────────────────────────

// Получить кол-во рабочих дней между двумя датами (пн-пт)
function getWorkingDaysBetween(startTs, endTs) {
  let count = 0;
  const start = new Date(startTs * 1000);
  const end = new Date(endTs * 1000);
  const cur = new Date(start);
  cur.setHours(0, 0, 0, 0);
  end.setHours(0, 0, 0, 0);
  while (cur < end) {
    const day = cur.getDay();
    if (day !== 0 && day !== 6) count++;
    cur.setDate(cur.getDate() + 1);
  }
  return count;
}

// ─── AMO DATA FETCHERS ────────────────────────────────────────────────────────

async function getPipelineAndStage() {
  const { data } = await amo.get('/leads/pipelines');
  const pipelines = data._embedded?.pipelines || [];
  const pipeline = pipelines.find(p => p.name === PIPELINE_NAME);
  if (!pipeline) throw new Error(`Воронка "${PIPELINE_NAME}" не найдена`);
  const stage = pipeline._embedded?.statuses?.find(s => s.name === STAGE_NAME);
  if (!stage) throw new Error(`Этап "${STAGE_NAME}" не найден`);
  return { pipelineId: pipeline.id, stageId: stage.id };
}

async function getGroupUsers() {
  // Получаем пользователей через настройки виджета (хранятся в кастомном поле)
  // Либо fallback - из переменной окружения
  const envUsers = process.env.QUEUE_USERS; // JSON: [{"id":123,"name":"Иванов"}]
  if (envUsers) {
    try { return JSON.parse(envUsers); } catch(e) {}
  }
  // Запрашиваем всех пользователей и фильтруем по группе
  const { data } = await amo.get('/users?limit=250');
  const users = data._embedded?.users || [];
  return users.filter(u => u.group?.name === GROUP_NAME).map(u => ({ id: u.id, name: u.name }));
}

async function getLeadsInStage(pipelineId, stageId) {
  let page = 1;
  let leads = [];
  while (true) {
    const { data } = await amo.get(`/leads?filter[pipeline_id]=${pipelineId}&filter[status_id]=${stageId}&limit=250&page=${page}`);
    const batch = data._embedded?.leads || [];
    leads = leads.concat(batch);
    if (batch.length < 250) break;
    page++;
  }
  return leads;
}

async function createTask(leadId, responsibleUserId, text) {
  const dueDate = Math.floor(Date.now() / 1000) + 86400; // завтра
  await amo.post('/tasks', [{
    task_type_id: 1,
    text,
    complete_till: dueDate,
    entity_id: leadId,
    entity_type: 'leads',
    responsible_user_id: responsibleUserId
  }]);
  console.log(`✅ Задача создана для сделки ${leadId}`);
}

async function getExistingTasks(leadId) {
  const { data } = await amo.get(`/tasks?filter[entity_id]=${leadId}&filter[entity_type]=leads`);
  return data._embedded?.tasks || [];
}

async function reassignLead(leadId, newUserId, newUserName) {
  await amo.patch(`/leads`, [{
    id: leadId,
    responsible_user_id: newUserId
  }]);
  // Добавляем примечание о переназначении
  await amo.post('/notes', [{
    entity_id: leadId,
    entity_type: 'leads',
    note_type: 'common',
    params: { text: `🔄 Автоматическое переназначение: лид висел в "Недозвон" 5 рабочих дней. Новый ответственный: ${newUserName}` }
  }]);
  console.log(`🔄 Сделка ${leadId} переназначена на ${newUserName} (id: ${newUserId})`);
}

// ─── QUEUE STATE ──────────────────────────────────────────────────────────────
// Хранит индекс следующего в очереди в памяти (сбрасывается при рестарте)
// Для продакшна можно вынести в файл или БД
let queueIndex = 0;

function getNextUserFromQueue(users, currentUserId) {
  // Находим индекс текущего ответственного и берём следующего
  const currentIdx = users.findIndex(u => u.id === currentUserId);
  const nextIdx = (currentIdx + 1) % users.length;
  queueIndex = nextIdx;
  return users[nextIdx];
}

// ─── MAIN CHECK ───────────────────────────────────────────────────────────────

async function checkLeads() {
  console.log(`\n[${new Date().toISOString()}] Запуск проверки сделок...`);
  
  try {
    const { pipelineId, stageId } = await getPipelineAndStage();
    const users = await getGroupUsers();
    
    if (!users.length) {
      console.log('⚠️ Сотрудники не найдены, пропускаем');
      return;
    }
    
    const userIds = new Set(users.map(u => u.id));
    const leads = await getLeadsInStage(pipelineId, stageId);
    const nowTs = Math.floor(Date.now() / 1000);
    
    console.log(`Найдено ${leads.length} сделок в этапе "${STAGE_NAME}"`);
    
    for (const lead of leads) {
      const responsibleId = lead.responsible_user_id;
      
      // Только сотрудники из нужного отдела
      if (!userIds.has(responsibleId)) continue;
      
      // status_changed_at — когда сделка попала в текущий этап
      const stageEnteredAt = lead.status_changed_at || lead.created_at;
      const workingDays = getWorkingDaysBetween(stageEnteredAt, nowTs);
      
      console.log(`Сделка ${lead.id} "${lead.name}": ${workingDays} рабочих дней в этапе`);
      
      // 5+ рабочих дней → переназначить
      if (workingDays >= 5) {
        const nextUser = getNextUserFromQueue(users, responsibleId);
        await reassignLead(lead.id, nextUser.id, nextUser.name);
        continue;
      }
      
      // 3+ рабочих дней → создать задачу (если ещё не создавали)
      if (workingDays >= 3) {
        const tasks = await getExistingTasks(lead.id);
        const reminderExists = tasks.some(t => t.text?.includes('Что делаем с лидом'));
        if (!reminderExists) {
          await createTask(
            lead.id,
            responsibleId,
            `⏰ Что делаем с лидом? Сделка "${lead.name}" уже ${workingDays} рабочих дней в этапе "Недозвон". Примите решение.`
          );
        }
      }
    }
    
    console.log('✅ Проверка завершена\n');
  } catch (err) {
    console.error('❌ Ошибка при проверке:', err.message);
  }
}

// ─── CRON: каждые 2 часа в рабочее время пн-пт ───────────────────────────────
cron.schedule('0 9,11,13,15,17 * * 1-5', checkLeads, {
  timezone: 'Europe/Moscow'
});

// ─── ROUTES ───────────────────────────────────────────────────────────────────

// Вебхук от AmoCRM (для будущего использования)
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
});

// Получить текущий список сотрудников в очереди
app.get('/api/users', async (req, res) => {
  try {
    const users = await getGroupUsers();
    res.json({ success: true, users, currentQueueIndex: queueIndex });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// Обновить список сотрудников (вызывается из панели виджета)
app.post('/api/users', express.json(), (req, res) => {
  const { users } = req.body;
  if (!Array.isArray(users)) return res.status(400).json({ error: 'users must be array' });
  // Сохраняем в env (на Render через переменные окружения)
  process.env.QUEUE_USERS = JSON.stringify(users);
  queueIndex = 0;
  res.json({ success: true, users });
});

// Ручной запуск проверки (для дебага)
app.post('/api/check', async (req, res) => {
  checkLeads();
  res.json({ success: true, message: 'Проверка запущена' });
});

// Health check
app.get('/', (req, res) => res.json({ status: 'ok', time: new Date().toISOString() }));

app.listen(PORT, () => {
  console.log(`🚀 Сервер запущен на порту ${PORT}`);
  // Запускаем первую проверку через 5 сек после старта
  setTimeout(checkLeads, 5000);
});
