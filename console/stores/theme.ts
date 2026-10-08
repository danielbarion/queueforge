"use client";

import { create } from "zustand";
import { persist } from "zustand/middleware";

export type ThemePreference = "system" | "dark" | "light";
export type ThemeName = "queueforge" | "queueforge-light";

type ThemeState = {
  preference: ThemePreference;
  setPreference: (preference: ThemePreference) => void;
};

export function resolvedTheme(preference: ThemePreference, systemDark: boolean): ThemeName {
  if (preference === "light") return "queueforge-light";
  if (preference === "dark") return "queueforge";
  return systemDark ? "queueforge" : "queueforge-light";
}

export const useThemeStore = create<ThemeState>()(
  persist(
    (set) => ({
      preference: "system",
      setPreference: (preference) => set({ preference }),
    }),
    {
      name: "queueforge-console-theme",
      version: 2,
      migrate: (persisted) => {
        const saved = persisted as { preference?: string; theme?: string };
        if (saved.preference === "system" || saved.preference === "dark" || saved.preference === "light") {
          return { preference: saved.preference };
        }
        if (saved.theme === "queueforge-light") return { preference: "light" };
        if (saved.theme === "queueforge") return { preference: "dark" };
        return { preference: "system" };
      },
    },
  ),
);
