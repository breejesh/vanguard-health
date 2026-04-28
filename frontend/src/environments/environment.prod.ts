export const environment = {
  production: true,
  duckdbAssetCacheKey: '20260428',
  parquetSource: 'local', // 'local' or 'firebase-cdn'
  parquetSources: {
    local: {
      baseUrl: '/assets/gold',
    },
    'firebase-cdn': {
      baseUrl: 'https://vanguard-health.web.app/gold',
    },
  },
  parquetFiles: {
    conditionsParquet: '_conditions.parquet',
    h3ReferenceParquet: '_h3_reference.parquet',
    conditionParquetTemplate: '{condition}.parquet',
  },
  duckdbBundles: {
    mvp: {
      mainModule: 'assets/duckdb/duckdb-mvp.wasm?v=20260428',
      mainWorker: 'assets/duckdb/duckdb-browser-mvp.worker.js?v=20260428',
      pthreadWorker: null,
    },
  },
  duckdbBundlePriority: ['local'],
};
