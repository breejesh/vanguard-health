export interface LocalDuckDbBundle {
  mainModule: string;
  mainWorker: string;
  pthreadWorker: string | null;
}