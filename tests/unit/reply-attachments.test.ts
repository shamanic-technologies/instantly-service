import { createHash } from "node:crypto";

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
const mockInsertValues = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockDbExecute(...a),
    insert: () => ({ values: (...a: unknown[]) => mockInsertValues(...a) }),
  },
}));
vi.mock("../../src/db/schema", () => ({ smtpDispatchRaw: { id: "id" } }));

const mockListEmails = vi.fn();
const mockReplyToEmail = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  listEmails: (...a: unknown[]) => mockListEmails(...a),
  getAccount: vi.fn(),
  replyToEmail: (...a: unknown[]) => mockReplyToEmail(...a),
}));
vi.mock("../../src/lib/key-client", () => ({ resolveInstantlyApiKey: vi.fn() }));
vi.mock("../../src/lib/sales-rep-copy", () => ({ salesRepCopyList: async () => [] }));

const mockResolveCredential = vi.fn();
vi.mock("../../src/lib/self-send/mailbox-credentials", () => ({
  resolveMailboxCredential: (...a: unknown[]) => mockResolveCredential(...a),
}));
const mockDispatchMessage = vi.fn();
vi.mock("../../src/lib/self-send/smtp", () => ({
  dispatchMessage: (...a: unknown[]) => mockDispatchMessage(...a),
}));
vi.mock("../../src/lib/reply-destination", () => ({
  findProspectWroteElsewhere: async () => null,
}));

import { replyToLead } from "../../src/lib/reply-to-lead";
import {
  filenameFromDisposition,
  MAX_REPLY_ATTACHMENT_BYTES,
  resolveReplyAttachments,
} from "../../src/lib/offer-documents";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

const PDF = Buffer.from("%PDF-1.7 case study bytes");
const PDF_MD5 = createHash("md5").update(PDF).digest("hex");

const INPUT = {
  orgId: "org-1",
  userId: "user-1",
  campaignId: "camp-1",
  leadEmail: "alice@media.com",
  bodyHtml: "<p>Here is the case study.</p>",
  sentBy: "human" as const,
};

function primeSelfSend() {
  mockDbExecute
    .mockResolvedValueOnce(
      pgResult([
        {
          campaignId: "camp-1",
          instantlyCampaignId: "self:abc",
          leadEmail: "alice@media.com",
          accountEmail: "amy@boostdistribute.com",
          sendTransport: "smtp",
        },
      ]),
    )
    .mockResolvedValueOnce(
      pgResult([{ messageId: "<their-2@mail>", subject: "Quick question", at: "2026-09-01" }]),
    )
    .mockResolvedValueOnce(pgResult([{ messageId: "<ours-1@mail>" }, { messageId: "<their-2@mail>" }]))
    .mockResolvedValueOnce(pgResult([{ email: "amy@boostdistribute.com", firstName: "Amy", lastName: "Moore" }]));
}

function documentResponse(
  bytes = PDF,
  filename = "Case study.pdf",
  sha256 = createHash("sha256").update(bytes).digest("hex"),
): Response {
  return new Response(bytes, {
    status: 200,
    headers: {
      "content-type": "application/pdf",
      "content-disposition": `attachment; filename="${filename}"`,
      "x-document-sha256": sha256,
    },
  });
}

const fetchMock = vi.fn();

beforeEach(() => {
  vi.resetAllMocks();
  process.env.BRAND_SERVICE_URL = "https://brand.example";
  process.env.BRAND_SERVICE_API_KEY = "brand-key";
  vi.stubGlobal("fetch", fetchMock);
  mockInsertValues.mockResolvedValue(undefined);
  mockResolveCredential.mockResolvedValue({
    address: "amy@boostdistribute.com",
    appPassword: "pw",
    smtpHost: "smtp.gmail.com",
    imapHost: "imap.gmail.com",
  });
  mockDispatchMessage.mockResolvedValue({
    messageId: "<ours-3@mail>",
    response: "250 OK",
    accepted: ["alice@media.com"],
    rejected: [],
  });
});

afterEach(() => vi.unstubAllGlobals());

describe("a reply carrying an offer document", () => {
  it("attaches brand-service's exact bytes, in the same thread, and records the file", async () => {
    primeSelfSend();
    fetchMock.mockResolvedValueOnce(documentResponse());

    const outcome = await replyToLead({ ...INPUT, attachmentDocumentIds: ["doc-1"] });

    // Read as THIS org, server to server.
    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toContain("doc-1");
    expect((init as RequestInit).headers).toMatchObject({ "x-org-id": "org-1", "x-api-key": "brand-key" });

    const message = mockDispatchMessage.mock.calls[0][1];
    expect(message.inReplyTo).toBe("<their-2@mail>");
    expect(message.references).toEqual(["<ours-1@mail>", "<their-2@mail>"]);
    expect(message.attachments).toHaveLength(1);
    expect(message.attachments[0].filename).toBe("Case study.pdf");
    expect(message.attachments[0].contentType).toBe("application/pdf");
    expect(Buffer.compare(message.attachments[0].content, PDF)).toBe(0);

    const described = {
      documentId: "doc-1",
      filename: "Case study.pdf",
      contentType: "application/pdf",
      sizeBytes: PDF.length,
      md5: PDF_MD5,
    };
    expect(outcome.reply.attachments).toEqual([described]);
    const row = mockInsertValues.mock.calls[0][0];
    expect(row.payload.attachments).toEqual([described]);
  });

  it("a reply with no document is exactly the reply it was before", async () => {
    primeSelfSend();

    const outcome = await replyToLead(INPUT);

    expect(fetchMock).not.toHaveBeenCalled();
    expect("attachments" in mockDispatchMessage.mock.calls[0][1]).toBe(false);
    expect("attachments" in outcome.reply).toBe(false);
    expect("attachments" in mockInsertValues.mock.calls[0][0].payload).toBe(false);
  });

  it("another org's (or an unknown) document refuses the reply, nothing sent", async () => {
    primeSelfSend();
    fetchMock.mockResolvedValueOnce(new Response("not found", { status: 404 }));

    await expect(
      replyToLead({ ...INPUT, attachmentDocumentIds: ["other-org-doc"] }),
    ).rejects.toMatchObject({ code: "attachment_not_found", status: 404 });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
    expect(mockInsertValues).not.toHaveBeenCalled();
  });

  it("a brand-service failure refuses the reply as unavailable, nothing sent", async () => {
    primeSelfSend();
    fetchMock.mockResolvedValueOnce(new Response("boom", { status: 500 }));

    await expect(
      replyToLead({ ...INPUT, attachmentDocumentIds: ["doc-1"] }),
    ).rejects.toMatchObject({ code: "attachment_unavailable", status: 502 });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });

  it("bytes that are not the document brand-service states refuse the reply", async () => {
    primeSelfSend();
    fetchMock.mockResolvedValueOnce(documentResponse(PDF, "Case study.pdf", "0".repeat(64)));

    await expect(
      replyToLead({ ...INPUT, attachmentDocumentIds: ["doc-1"] }),
    ).rejects.toMatchObject({ code: "attachment_unavailable", status: 502 });
    expect(mockDispatchMessage).not.toHaveBeenCalled();
  });

  it("a thread on the Instantly pipe refuses a file before reading any document", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([
        {
          campaignId: "camp-1",
          instantlyCampaignId: "ic-1",
          leadEmail: "alice@media.com",
          accountEmail: "amy@boostdistribute.com",
          sendTransport: "instantly",
        },
      ]),
    );

    await expect(
      replyToLead({ ...INPUT, attachmentDocumentIds: ["doc-1"] }),
    ).rejects.toMatchObject({ code: "attachments_unsupported_on_transport", status: 422 });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(mockReplyToEmail).not.toHaveBeenCalled();
  });
});

describe("resolveReplyAttachments", () => {
  it("refuses documents over the size limit together", async () => {
    const big = Buffer.alloc(MAX_REPLY_ATTACHMENT_BYTES / 2 + 1);
    fetchMock.mockImplementation(async () => documentResponse(big));

    await expect(resolveReplyAttachments("org-1", ["a", "b"])).rejects.toMatchObject({
      code: "attachment_too_large",
      status: 422,
    });
  });

  it("refuses more than five documents without reading any", async () => {
    await expect(
      resolveReplyAttachments("org-1", ["1", "2", "3", "4", "5", "6"]),
    ).rejects.toMatchObject({ code: "attachment_too_large" });
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("one id named twice is one file", async () => {
    fetchMock.mockResolvedValueOnce(documentResponse());
    const files = await resolveReplyAttachments("org-1", ["doc-1", "doc-1"]);
    expect(files).toHaveLength(1);
  });
});

describe("filenameFromDisposition", () => {
  it("reads filename* first, then filename", () => {
    expect(filenameFromDisposition(`attachment; filename="a.pdf"; filename*=UTF-8''%C3%A9t%C3%A9.pdf`)).toBe("été.pdf");
    expect(filenameFromDisposition(`attachment; filename="One pager.pdf"`)).toBe("One pager.pdf");
    expect(filenameFromDisposition(null)).toBeNull();
  });
});
