// This file can be replaced during build by using the `fileReplacements` array.
// `ng build` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.

export const environment = {
  production: false,
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
    eh: {
      mainModule: 'assets/duckdb/duckdb-eh.wasm?v=20260428',
      mainWorker: 'assets/duckdb/duckdb-browser-eh.worker.js?v=20260428',
      pthreadWorker: null,
    },
  },
  duckdbBundlePriority: ['local'],
};

/*
 * For easier debugging in development mode, you can import the following file
 * to ignore zone related error stack frames such as `zone.run`, `zoneDelegate.invokeTask`.
 *
 * This import should be commented out in production mode because it will have a negative impact
 * on performance if an error is thrown.
 */
// import 'zone.js/plugins/zone-error';  // Included with Angular CLI.
