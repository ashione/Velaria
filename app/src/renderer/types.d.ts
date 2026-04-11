export {};

declare global {
  interface Window {
    velariaShell: {
      pickFile: () => Promise<string | null>;
      getServiceInfo: () => Promise<{ baseUrl: string; packaged: boolean }>;
      exportFile: (payload: { sourcePath: string; suggestedName?: string }) => Promise<{
        cancelled: boolean;
        destination?: string;
      }>;
    };
  }
}
