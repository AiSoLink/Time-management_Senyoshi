import "./globals.css";
import { UploadFilesProvider } from "@/components/UploadFilesContext";

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ja">
      <body style={{ fontFamily: "system-ui, sans-serif", margin: 0 }}>
        <UploadFilesProvider>
          <div style={{ padding: 16, maxWidth: 980, margin: "0 auto" }}>
            <h1 style={{ margin: "8px 0 16px" }}>時間管理システム</h1>
            {children}
          </div>
        </UploadFilesProvider>
      </body>
    </html>
  );
}
