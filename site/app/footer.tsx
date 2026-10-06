import { BENCH_MD, GITHUB } from "./bench";

export function Footer() {
  return (
    <footer>
      <div className="wrap">
        <p>QueueForge. Open source. You run the process. The rates are measurements, not a target.</p>
        <nav aria-label="Repository">
          <a href={GITHUB}>GitHub</a>
          <a href={`${GITHUB}/blob/main/README.md`}>README</a>
          <a href={BENCH_MD}>BENCHMARK.md</a>
        </nav>
      </div>
    </footer>
  );
}
