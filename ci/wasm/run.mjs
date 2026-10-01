// Runs every function that the check module exports, in Node. Each returns 0 where the locks
// behaved as expected and the number of the step that went wrong otherwise; a panic, such as
// the standard library's clock giving up on `wasm32-unknown-unknown`, traps instead.
import { readFile } from "node:fs/promises";

const [path] = process.argv.slice(2);
const { instance } = await WebAssembly.instantiate(await readFile(path));
let failed = false;
for (const [name, check] of Object.entries(instance.exports)) {
  if (typeof check !== "function") {
    continue;
  }
  try {
    const step = check();
    if (step === 0) {
      console.log(`ok: ${name}`);
    } else {
      console.error(`FAILED: ${name}, at step ${step}`);
      failed = true;
    }
  } catch (error) {
    console.error(`FAILED: ${name} trapped: ${error}`);
    failed = true;
  }
}
process.exit(failed ? 1 : 0);
