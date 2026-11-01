import { randomUUID } from "node:crypto";
import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  INestApplication,
  Logger,
  ValidationPipe,
} from "@nestjs/common";
import helmet from "helmet";
import { Request, Response, NextFunction } from "express";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";

@Catch()
export class Errors implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost) {
    const response = host.switchToHttp().getResponse<Response>();
    const status =
      error instanceof HttpException
        ? error.getStatus()
        : host.switchToHttp().getRequest<Request>().path === "/health/ready"
          ? HttpStatus.SERVICE_UNAVAILABLE
          : HttpStatus.INTERNAL_SERVER_ERROR;
    if (status === 500)
      new Logger("Errors").error(
        error instanceof Error ? error.stack : "Unhandled error",
      );
    response.status(status).json({
      statusCode: status,
      error:
        status === 500
          ? "Internal server error"
          : error instanceof HttpException
            ? error.getResponse()
            : "Request failed",
      requestId: response.getHeader("x-request-id"),
    });
  }
}
export function configure(app: INestApplication) {
  app.use(helmet());
  app.enableCors({
    origin: process.env.CORS_ORIGIN?.split(",") ?? [],
    credentials: true,
  });
  app.use((req: Request, res: Response, next: NextFunction) => {
    const supplied = req.header("x-request-id");
    const requestId =
      supplied && /^[a-zA-Z0-9-]{1,80}$/.test(supplied)
        ? supplied
        : randomUUID();
    res.setHeader("x-request-id", requestId);
    const start = performance.now();
    res.on("finish", () =>
      console.log(
        JSON.stringify({
          requestId,
          method: req.method,
          path: req.path,
          status: res.statusCode,
          durationMs: Math.round(performance.now() - start),
        }),
      ),
    );
    next();
  });
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );
  app.useGlobalFilters(new Errors());
  app.enableShutdownHooks();
  const document = SwaggerModule.createDocument(
    app,
    new DocumentBuilder()
      .setTitle("event-driven-orders")
      .setVersion("1")
      .addBearerAuth()
      .build(),
  );
  SwaggerModule.setup("docs", app, document);
}
export function port(): number {
  const value = Number(process.env.PORT ?? 3000);
  if (!Number.isInteger(value) || value < 1 || value > 65535)
    throw new Error("PORT must be between 1 and 65535");
  return value;
}
