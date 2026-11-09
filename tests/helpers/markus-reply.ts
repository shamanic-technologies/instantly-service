/**
 * Verbatim shape of the prod row 80d977dd-1486-4959-8b64-05c7bc6a3ecf
 * (2026-11-09, Living Vital): our cold email went to Thomas; his colleague
 * Markus answered in the same thread. Headers as our IMAP poll stores them
 * (mailparser objects serialised to JSON strings).
 */
export const MARKUS_IMAP_ROW = {
  id: "80d977dd-1486-4959-8b64-05c7bc6a3ecf",
  account_email: "kevin@marketingagency.group",
  message_id: "<CAE-KvCN+LJ7BBKX=zE_pgiy0rzU5u=BJOHcJKfoMb0DRqk5Zkg@mail.gmail.com>",
  from_address: '"Markus Schmid" <markus@markthalle-luzern.ch>',
  subject: "Re: Flohsamenschalen für die Markthalle",
  kind: "reply",
  instantly_campaign_id: "self:fd6b0eb8-bed3-4a0d-80f0-5b83f9633578",
  step: 1,
  received_at: new Date("2026-11-09T15:28:51.000Z"),
  polled_at: new Date("2026-11-09T15:30:02.000Z"),
  payload: {
    from: '"Markus Schmid" <markus@markthalle-luzern.ch>',
    subject: "Re: Flohsamenschalen für die Markthalle",
    textSnippet: "Hallo Kevin, werden die Flohsamenschalen in der Schweiz angebaut? Gruss Markus",
    referencedMessageIds: ["<5ac53340-e14c-ac34-7685-52a54368fbe2@marketingagency.group>"],
    headers: {
      to: JSON.stringify({
        value: [{ address: "kevin@marketingagency.group", name: "" }],
        text: "kevin@marketingagency.group",
      }),
      from: JSON.stringify({
        value: [{ address: "markus@markthalle-luzern.ch", name: "Markus Schmid" }],
        text: '"Markus Schmid" <markus@markthalle-luzern.ch>',
      }),
      "message-id": "<CAE-KvCN+LJ7BBKX=zE_pgiy0rzU5u=BJOHcJKfoMb0DRqk5Zkg@mail.gmail.com>",
      "in-reply-to": "<CAPfU9LnYs30w-Q30SrLD87Ase-eBjUb4K64_JsAwSjXCHHfwyA@mail.gmail.com>",
      references: JSON.stringify([
        "<5ac53340-e14c-ac34-7685-52a54368fbe2@marketingagency.group>",
        "<CAPfU9LnYs30w-Q30SrLD87Ase-eBjUb4K64_JsAwSjXCHHfwyA@mail.gmail.com>",
      ]),
    },
  },
  c_org_id: "8bfec2f4-6184-40b7-b1aa-c8933d506a87",
  c_brand_ids: ["f2408cfb-4f02-4910-acec-e61fc8edb9cf"],
  c_campaign_id: "camp-living-vital",
  c_instantly_campaign_id: "self:fd6b0eb8-bed3-4a0d-80f0-5b83f9633578",
  c_lead_email: "thomas@markthalle-luzern.ch",
};
