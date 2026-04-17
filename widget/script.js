define(['jquery'], function($) {
  var Widget = function() {
    var self = this;
   var BACKEND_URL = 'https://nedozvon-21-century.onrender.com';

    this.callbacks = {
      render: function() {
        return true;
      },

      init: function() {
        return true;
      },

      bind_actions: function() {
        return true;
      },

      // Панель настроек виджета
      settings: function() {
        var $container = $('.widget-settings__fields');
        $container.html('');

        var html = `
          <div style="padding: 20px; font-family: sans-serif;">
            <h3 style="margin-bottom: 16px; font-size: 15px; font-weight: 600;">
              ⚙️ Настройки виджета "Недозвон"
            </h3>

            <div style="margin-bottom: 20px; padding: 12px; background: #f5f7fa; border-radius: 6px; font-size: 13px; color: #555;">
              <b>Условия срабатывания:</b><br>
              • День 3 (рабочих): задача "Что делаем с лидом?"<br>
              • День 5 (рабочих): автоперераспределение по очереди
            </div>

            <div style="margin-bottom: 16px;">
              <label style="font-size: 13px; font-weight: 600; display: block; margin-bottom: 8px;">
                Сотрудники в очереди (Клуб чемпионов):
              </label>
              <div id="users-list" style="margin-bottom: 10px;"></div>
              <div style="display: flex; gap: 8px;">
                <select id="user-select" style="flex: 1; padding: 6px; border: 1px solid #ddd; border-radius: 4px; font-size: 13px;">
                  <option value="">Загрузка сотрудников...</option>
                </select>
                <button id="add-user-btn" style="padding: 6px 14px; background: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px;">
                  + Добавить
                </button>
              </div>
            </div>

            <div style="display: flex; gap: 10px; margin-top: 20px;">
              <button id="save-btn" style="padding: 8px 20px; background: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; font-weight: 600;">
                💾 Сохранить
              </button>
              <button id="check-btn" style="padding: 8px 20px; background: #FF9800; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px;">
                ▶ Запустить проверку сейчас
              </button>
            </div>

            <div id="status-msg" style="margin-top: 12px; font-size: 13px;"></div>
          </div>
        `;

        $container.html(html);

        var selectedUsers = [];
        var allUsers = [];

        // Загружаем текущих пользователей из бэкенда
        $.get(BACKEND_URL + '/api/users', function(data) {
          selectedUsers = data.users || [];
          renderUsersList();
        }).fail(function() {
          showStatus('⚠️ Не удалось подключиться к бэкенду', 'red');
        });

        // Загружаем всех пользователей AmoCRM
        $.get('/api/v4/users?limit=250', function(data) {
          allUsers = data._embedded?.users || [];
          var $select = $('#user-select');
          $select.html('<option value="">Выберите сотрудника...</option>');
          allUsers.forEach(function(u) {
            $select.append(`<option value="${u.id}" data-name="${u.name}">${u.name}</option>`);
          });
        });

        function renderUsersList() {
          var $list = $('#users-list');
          if (!selectedUsers.length) {
            $list.html('<div style="font-size: 13px; color: #999; margin-bottom: 8px;">Нет добавленных сотрудников</div>');
            return;
          }
          var html = selectedUsers.map(function(u, idx) {
            return `
              <div style="display: flex; align-items: center; justify-content: space-between; padding: 6px 10px; background: white; border: 1px solid #e0e0e0; border-radius: 4px; margin-bottom: 6px;">
                <span style="font-size: 13px;">
                  <span style="color: #999; margin-right: 8px;">${idx + 1}.</span>
                  ${u.name}
                </span>
                <button data-id="${u.id}" class="remove-user-btn" style="background: none; border: none; color: #f44336; cursor: pointer; font-size: 16px; padding: 0 4px;">×</button>
              </div>
            `;
          }).join('');
          $list.html(html);

          $('.remove-user-btn').on('click', function() {
            var id = parseInt($(this).data('id'));
            selectedUsers = selectedUsers.filter(function(u) { return u.id !== id; });
            renderUsersList();
          });
        }

        // Добавить сотрудника
        $('#add-user-btn').on('click', function() {
          var $select = $('#user-select');
          var id = parseInt($select.val());
          var name = $select.find(':selected').data('name');
          if (!id) return;
          if (selectedUsers.find(function(u) { return u.id === id; })) {
            showStatus('⚠️ Этот сотрудник уже добавлен', 'orange');
            return;
          }
          selectedUsers.push({ id: id, name: name });
          renderUsersList();
          $select.val('');
        });

        // Сохранить
        $('#save-btn').on('click', function() {
          if (!selectedUsers.length) {
            showStatus('⚠️ Добавьте хотя бы одного сотрудника', 'orange');
            return;
          }
          $.ajax({
            url: BACKEND_URL + '/api/users',
            method: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({ users: selectedUsers }),
            success: function() {
              showStatus('✅ Сохранено успешно!', 'green');
            },
            error: function() {
              showStatus('❌ Ошибка сохранения', 'red');
            }
          });
        });

        // Ручной запуск проверки
        $('#check-btn').on('click', function() {
          showStatus('⏳ Запускаем проверку...', '#555');
          $.post(BACKEND_URL + '/api/check', function() {
            showStatus('✅ Проверка запущена! Результаты смотри в логах Render.', 'green');
          }).fail(function() {
            showStatus('❌ Ошибка запуска', 'red');
          });
        });

        function showStatus(msg, color) {
          $('#status-msg').html(`<span style="color: ${color}">${msg}</span>`);
        }

        return true;
      },

      onSave: function() {
        return true;
      },

      destroy: function() {
        return true;
      }
    };
  };

  return Widget;
});
