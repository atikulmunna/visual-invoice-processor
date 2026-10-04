import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, Navigate, RouterProvider } from "react-router";

import { KitPage } from "./pages/KitPage";
import { OverviewPage } from "./pages/OverviewPage";
import { NotFoundPage, RecordsPage, ReviewPage, UploadPage } from "./pages/PlaceholderPages";
import { SettingsPage } from "./pages/SettingsPage";
import { AppShell } from "./shell/AppShell";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/components.css";
import "./styles/shell.css";

const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <AppShell />,
      children: [
        { index: true, element: <Navigate to="/overview" replace /> },
        { path: "overview", element: <OverviewPage /> },
        { path: "records", element: <RecordsPage /> },
        { path: "review", element: <ReviewPage /> },
        { path: "upload", element: <UploadPage /> },
        { path: "settings", element: <SettingsPage /> },
        { path: "kit", element: <KitPage /> },
        { path: "*", element: <NotFoundPage /> },
      ],
    },
  ],
  { basename: "/app" },
);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);
