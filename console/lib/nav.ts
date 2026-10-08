import {
  Activity,
  ArrowLeftRight,
  Bell,
  Boxes,
  Cable,
  Flag,
  Gauge,
  Layers,
  Network,
  Radio,
  ScrollText,
  Server,
  Shield,
  Users,
  type LucideIcon,
} from "lucide-react";

export type NavItem = {
  href: string;
  label: string;
  icon: LucideIcon;
};

export type NavGroup = {
  section: string | null;
  items: NavItem[];
};

export const NAV: NavGroup[] = [
  {
    section: null,
    items: [
      { href: "/", label: "Overview", icon: Gauge },
      { href: "/brokers", label: "Brokers", icon: Boxes },
      { href: "/activity", label: "Activity", icon: Activity },
    ],
  },
  {
    section: "Operate",
    items: [
      { href: "/queues", label: "Queues", icon: Layers },
      { href: "/exchanges", label: "Exchanges", icon: ArrowLeftRight },
      { href: "/connections", label: "Connections", icon: Cable },
      { href: "/channels", label: "Channels", icon: Radio },
    ],
  },
  {
    section: "Admin",
    items: [
      { href: "/vhosts", label: "Vhosts", icon: Network },
      { href: "/users", label: "Users", icon: Users },
      { href: "/policies", label: "Policies", icon: Shield },
      { href: "/feature-flags", label: "Feature flags", icon: Flag },
      { href: "/cluster", label: "Cluster", icon: Server },
      { href: "/definitions", label: "Definitions", icon: ScrollText },
      { href: "/alerts", label: "Alerts", icon: Bell },
    ],
  },
];
