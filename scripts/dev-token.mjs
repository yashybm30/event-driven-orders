import jwt from 'jsonwebtoken';
import {randomUUID} from 'node:crypto';
if(process.env.NODE_ENV==='production')throw new Error('Development token utility is disabled in production');
if(!process.env.JWT_SECRET||process.env.JWT_SECRET.length<48)throw new Error('Set the local JWT_SECRET first');
const [subject=randomUUID(),tenantId=randomUUID(),role='member']=process.argv.slice(2);
if(!/^[a-f0-9-]{36}$/.test(subject)||!/^[a-f0-9-]{36}$/.test(tenantId)||!['admin','member'].includes(role))throw new Error('Usage: token:dev -- subjectUUID tenantUUID admin|member');
console.error(`Local development identity: subject=${subject} tenant=${tenantId} role=${role}`);
console.log(jwt.sign({tenantId,role},process.env.JWT_SECRET,{subject,issuer:process.env.JWT_ISSUER||'portfolio-auth',audience:process.env.JWT_AUDIENCE||'portfolio-api',expiresIn:'15m'}));
