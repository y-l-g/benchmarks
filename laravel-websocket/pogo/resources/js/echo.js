import Echo from 'laravel-echo';
import Pusher from 'pusher-js';

window.Pusher = Pusher;

window.Echo = new Echo({
    broadcaster: 'pusher',
    key: 'pogo-key',
    cluster: 'mt1',
    wsHost: import.meta.env.VITE_POGO_HOST || window.location.hostname,
    wsPort: import.meta.env.VITE_POGO_PORT || 80,
    wssPort: import.meta.env.VITE_POGO_WSS_PORT || 443,
    forceTLS: false,
    disableStats: true,
    enabledTransports: ['ws', 'wss'],
    userAuthentication: {
        endpoint: "/pogo/user-auth"
    }
});