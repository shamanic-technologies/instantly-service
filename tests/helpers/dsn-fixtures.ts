/**
 * Delivery-status notices captured from prod, verbatim (BOUNCE-SPAM-BLOCK-1018).
 */

/**
 * GoDaddy refusing OUR message for its content, 2026-10-15 19:12 UTC, to
 * DefiniteJobs' own valid address (imap_messages_raw 9c5d0bdb). The enhanced
 * code 5.2.0 reads "mailbox" under RFC 3463: only the words say it is spam.
 */
export const GODADDY_SPAM_BLOCK_DSN = `
** Message blocked **

Your message to team@definitejobsgroup.co.uk has been blocked. See technical details below for more information.

Learn more here: https://checkspam.secureserver.net/?sid=HQsKx2oILWOUT&mid=HQsKx2oILWOUTHQsKx7dct
(Warning: This link will take you to a third-party site)

The response from the remote server was:
552 5.2.0 HQsKx2oILWOUT - HQsKx2oILWOUTHQsKx7dct This message has been rejected due to content judged to be spam by the internet community IB212 - If you feel this is in error, please submit a request using the following page. <https://checkspam.secureserver.net/?sid=HQsKx2oILWOUT&mid=HQsKx2oILWOUTHQsKx7dct>`;

/** Gmail's notice for an address that does not exist. */
export const GMAIL_ADDRESS_NOT_FOUND_DSN = `
** Address not found **

Your message wasn't delivered to jane.doe@example-clinic.com because the address couldn't be found, or is unable to receive mail.

Learn more here: https://support.google.com/mail/?p=NoSuchUser

The response from the remote server was:
550 5.1.1 The email account that you tried to reach does not exist. Please try double-checking the recipient's email address for typos or unnecessary spaces. https://support.google.com/mail/?p=NoSuchUser - gsmtp`;
