// k6 load scenario for POST /orders.
// Run (Docker):
//   docker run --rm -i --add-host=host.docker.internal:host-gateway \
//     -e BASE_URL=http://host.docker.internal:8081 \
//     -v ./load:/scripts grafana/k6 run /scripts/orders.js
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const accepted = new Counter("orders_accepted");

export const options = {
  stages: [
    { duration: "15s", target: 50 },
    { duration: "30s", target: 150 },
    { duration: "15s", target: 0 },
  ],
  thresholds: {
    http_req_failed: ["rate<0.001"],
    http_req_duration: ["p(99)<250"],
  },
};

const BASE = __ENV.BASE_URL || "http://localhost:8081";
const STORES = ["waluigis", "taco-tornado", "store-3", "store-4"];
const ITEMS = [
  ["The WAH Supreme - Medium", 1799],
  ["Purple Heat - Large", 2099],
  ["Tornado Trio plate", 1399],
  ["Garlic knots (6)", 599],
  ["Horchata", 399],
];

export default function () {
  const lineCount = 1 + Math.floor(Math.random() * 3);
  const lines = [];
  let total = 0;
  for (let i = 0; i < lineCount; i++) {
    const [name, price] = ITEMS[Math.floor(Math.random() * ITEMS.length)];
    const qty = 1 + Math.floor(Math.random() * 3);
    lines.push({ name, quantity: qty, unitPriceCents: price });
    total += price * qty;
  }
  const body = JSON.stringify({
    storeId: STORES[Math.floor(Math.random() * STORES.length)],
    customer: { name: `Load VU${__VU}`, phone: "5550100000" },
    lines,
    totalCents: total,
  });
  const res = http.post(`${BASE}/orders`, body, {
    headers: { "Content-Type": "application/json" },
  });
  check(res, { "202": (r) => r.status === 202 });
  if (res.status === 202) accepted.add(1);
}
