import type { Metadata } from "next";
import "./styles.css";

export const metadata: Metadata = {
  title: "Atlas Energy",
  description: "Morocco energy-transition decision platform",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}

