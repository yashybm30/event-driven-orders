import { migrate } from "./database";
void migrate().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
