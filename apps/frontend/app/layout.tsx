import type { Metadata } from "next";
import type { ReactNode } from "react";
import "./globals.css";

import { NavBar } from "@/components/nav-bar";

export const metadata: Metadata = {
  title: "PurpleBank",
  description: "Nucleotide sequence database web app",
};

export default function RootLayout({ children }: { children: ReactNode }): JSX.Element {
  return (
    <html lang="en">
      <body>
        <div className="page-bg" />
        <div className="app-shell">
          <NavBar />
          <main>{children}</main>
        </div>
      </body>
    </html>
  );
}
