// Side-effect import: extends the shared Zod instance with `.openapi()` BEFORE any
// schema is created (including those imported from @shamanic-technologies/email-domain-contract).
// Zod 4 copies prototype methods onto instances at creation time, so the extension
// must be applied before any schema is instantiated — otherwise pre-existing schemas
// will not have `.openapi()` available.
//
// Always import this module BEFORE any module that creates Zod schemas:
//
//     import "./zod-setup";
//     import { ... } from "@shamanic-technologies/email-domain-contract";
//
import { z } from "zod";
import { extendZodWithOpenApi } from "@asteasolutions/zod-to-openapi";

extendZodWithOpenApi(z);
