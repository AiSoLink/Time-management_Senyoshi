"use client";

import { createContext, useContext, useState, useCallback, ReactNode } from "react";

export type LastUploadedFiles = {
  files: File[];
  taimenFiles: File[];
  alcoholFiles: File[];
};

type UploadFilesContextValue = {
  lastUploadedFiles: LastUploadedFiles | null;
  setLastUploadedFiles: (v: LastUploadedFiles | null) => void;
};

const UploadFilesContext = createContext<UploadFilesContextValue | null>(null);

export function UploadFilesProvider({ children }: { children: ReactNode }) {
  const [lastUploadedFiles, setLastUploadedFiles] = useState<LastUploadedFiles | null>(null);
  return (
    <UploadFilesContext.Provider value={{ lastUploadedFiles, setLastUploadedFiles }}>
      {children}
    </UploadFilesContext.Provider>
  );
}

export function useUploadFilesContext() {
  const ctx = useContext(UploadFilesContext);
  if (!ctx) throw new Error("useUploadFilesContext must be used within UploadFilesProvider");
  return ctx;
}
