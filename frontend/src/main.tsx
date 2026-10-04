import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, Navigate, RouterProvider } from "react-router";

import { KitPage } from "./pages/KitPage";
import { LoginPage } from "./pages/LoginPage";
import { OverviewPage } from "./pages/OverviewPage";
import { NotFoundPage, RecordsPage } from "./pages/PlaceholderPages";
import { ReviewQueuePage } from "./pages/ReviewQueuePage";
import { ReviewWorkspacePage } from "./pages/ReviewWorkspacePage";
import { SettingsPage } from "./pages/SettingsPage";
import { UploadPage } from "./pages/UploadPage";
import { AppShell } from "./shell/AppShell";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/components.css";
import "./styles/shell.css";
import "./styles/signin.css";
import "./styles/upload.css";
import "./styles/review.css";

const router = createBrowserRouter(
  [
    { path: "/login", element: <LoginPage /> },
    {
      path: "/",
      element: <AppShell />,
      children: [
        { index: true, element: <Navigate to="/overview" replace /> },
        { path: "overview", element: <OverviewPage /> },
        { path: "records", element: <RecordsPage /> },
        { path: "review", element: <ReviewQueuePage /> },
        { path: "review/:documentId", element: <ReviewWorkspacePage /> },
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
