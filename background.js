let socket = null;
let reconnectInterval = null;
let savedServerUrl = null;
let savedCode = null;
let savedDeviceToken = null;

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === 'CONNECT') {
    savedServerUrl = message.serverUrl;
    savedCode = message.code;
    savedDeviceToken = message.deviceToken;
    conectar();
  } else if (message.type === 'DISCONNECT') {
    if (reconnectInterval) clearInterval(reconnectInterval);
    if (socket) socket.close();
    socket = null;
  }
});

function conectar() {
  if (!savedServerUrl || !savedCode) return;
  if (socket && socket.readyState === WebSocket.OPEN) return;

  const wsUrl = savedServerUrl.replace(/^http/, 'ws') + `?code=${savedCode}&dt=${savedDeviceToken}`;
  socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    console.log('URLBridge: Conectado al servidor');
  };

  socket.onmessage = async (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.type === 'req') {
        try {
          const response = await fetch(msg.url, {
            method: msg.method || 'GET',
            headers: msg.headers || {},
            body: msg.method !== 'GET' && msg.method !== 'HEAD' ? JSON.stringify(msg.body) : undefined
          });

          const contentType = response.headers.get('content-type') || '';
          
          if (contentType.includes('text') || contentType.includes('json') || contentType.includes('javascript')) {
            const text = await response.text();
            socket.send(JSON.stringify({ type: 'res', id: msg.id, ok: true, contentType, text }));
          } else {
            const blob = await response.blob();
            const reader = new FileReader();
            reader.onloadend = () => {
              const base64data = reader.result.split(',')[1];
              socket.send(JSON.stringify({ type: 'res', id: msg.id, ok: true, contentType, base64: base64data }));
            };
            reader.readAsDataURL(blob);
          }
        } catch (err) {
          socket.send(JSON.stringify({ type: 'res', id: msg.id, ok: false, error: err.toString() }));
        }
      }
    } catch (e) {}
  };

  socket.onclose = () => {
    console.log('URLBridge: Desconectado. Reintentando en 3 segundos...');
    setTimeout(conectar, 3000); // Reconexión automática si se cae
  };

  socket.onerror = (err) => {
    console.error('Error en WebSocket:', err);
    socket.close();
  };
}