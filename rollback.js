const axios = require('axios');

const AMO_DOMAIN = 'c21pp.amocrm.ru';
const AMO_TOKEN = 'ВСТАВЬ_СВОЙ_ТОКЕН';

const amo = axios.create({
  baseURL: `https://${AMO_DOMAIN}/api/v4`,
  headers: { Authorization: `Bearer ${AMO_TOKEN}` }
});

// Маппинг имён в ID из данных которые мы получили ранее
const nameToId = {
  'Капанадзе Ольга': 13725758,
  'Кунцевич Виктория': 13695178,
  'Питаев Алексей': 13299234,
  'Хаджиева Амина': 13430842,
  'Данченкова Екатерина': 13430866,
  'Скуратова Валерия': 13503250,
  'Гордон Денис': 13515178,
  'Фатхуллов Рустем': 13324978,
  'Моджилло Татьяна': 13299178,
  'Матвеева Аделина': 13729438
};

async function getEventsToRollback() {
  const todayStart = Math.floor(new Date().setHours(0,0,0,0) / 1000);
  let page = 1;
  let events = [];
  
  while (true) {
    const { data } = await amo.get('/events', {
      params: {
        filter: { type: 'lead_responsible_user_changed' },
        'filter[created_at][from]': todayStart,
        limit: 100,
        page: page,
        with: 'lead'
      }
    });
    const batch = data._embedded?.events || [];
    events = events.concat(batch);
    if (batch.length < 100) break;
    page++;
  }
  return events;
}

async function rollback() {
  console.log('Получаем список изменений...');
  
  try {
    const { data } = await amo.get('/events', {
      params: {
        'filter[type]': 'lead_responsible_user_changed',
        'filter[created_at][from]': Math.floor(new Date().setHours(0,0,0,0) / 1000),
        limit: 250,
        page: 1
      }
    });
    
    const events = data._embedded?.events || [];
    console.log('Найдено событий: ' + events.length);
    
    const toRestore = {};
    
    for (const event of events) {
      const leadId = event.entity_id;
      const valueBefore = event.value_before?.[0]?.responsible_user?.name;
      
      if (!valueBefore || !nameToId[valueBefore]) {
        console.log('Пропускаем сделку ' + leadId + ' - не найден ' + valueBefore);
        continue;
      }
      
      // Берём только первое изменение для каждой сделки (самое раннее = исходный ответственный)
      if (!toRestore[leadId]) {
        toRestore[leadId] = nameToId[valueBefore];
      }
    }
    
    const entries = Object.entries(toRestore);
    console.log('Сделок для отката: ' + entries.length);
    
    // Откатываем пачками по 50
    for (let i = 0; i < entries.length; i += 50) {
      const batch = entries.slice(i, i + 50);
      const payload = batch.map(([id, userId]) => ({
        id: parseInt(id),
        responsible_user_id: userId
      }));
      
      await amo.patch('/leads', payload);
      console.log('Откатили ' + (i + batch.length) + ' из ' + entries.length);
      await new Promise(r => setTimeout(r, 500));
    }
    
    console.log('Откат завершён!');
  } catch (err) {
    console.error('Ошибка: ' + err.message);
    if (err.response) {
      console.error('Data: ' + JSON.stringify(err.response.data));
    }
  }
}

rollback();
