import path from "node:path";
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // The comparison workspace shares audited data with the website.
  turbopack: { root: path.resolve(__dirname, "..") },
};

export default nextConfig;
