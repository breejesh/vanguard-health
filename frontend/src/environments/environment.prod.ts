export const environment = {
  production: true,
  firebase: {
    // Frontend must use Firebase Web SDK config (apiKey/appId/etc),
    // never the Admin SDK service-account key.
    apiKey: '',
    authDomain: 'vanguard-health.firebaseapp.com',
    projectId: 'vanguard-health',
    // Optional for Firestore-only UI usage.
    storageBucket: '',
    messagingSenderId: '',
    appId: ''
  }
};
