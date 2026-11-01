import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import express from "express";
import { AppModule } from "./app";
import { configure, port } from "./platform";
import { configuration } from "./security";
async function main() {
  configuration();
  const app = await NestFactory.create(AppModule, { bodyParser: false });
  app.use(express.json({ limit: "64kb" }));
  configure(app);
  await app.listen(port(), "0.0.0.0");
}
void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
