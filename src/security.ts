import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { Request } from "express";
import jwt from "jsonwebtoken";
import { z } from "zod";
export const actorSchema = z.object({
  sub: z.string().uuid(),
  tenantId: z.string().uuid(),
  role: z.enum(["admin", "member"]),
});
export type Actor = z.infer<typeof actorSchema>;
export type AuthRequest = Request & { actor: Actor };
export function configuration() {
  if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 48)
    throw new Error("JWT_SECRET must contain at least 48 characters");
}
export function identity(token: string): Actor {
  try {
    return actorSchema.parse(
      actorSchema.extend({ exp: z.number().int().positive() }).parse(
        jwt.verify(token, process.env.JWT_SECRET!, {
          algorithms: ["HS256"],
          issuer: process.env.JWT_ISSUER ?? "portfolio-auth",
          audience: process.env.JWT_AUDIENCE ?? "portfolio-api",
        }),
      ),
    );
  } catch {
    throw new UnauthorizedException("Invalid access token");
  }
}
export function admin(actor: Actor) {
  if (actor.role !== "admin") throw new ForbiddenException("Admin required");
}
@Injectable()
export class AuthGuard implements CanActivate {
  canActivate(context: ExecutionContext) {
    const req = context.switchToHttp().getRequest<AuthRequest>();
    const header = req.header("authorization");
    if (!header?.startsWith("Bearer "))
      throw new UnauthorizedException("Bearer token required");
    req.actor = identity(header.slice(7));
    return true;
  }
}
