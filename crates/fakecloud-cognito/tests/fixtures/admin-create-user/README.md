# AdminCreateUser invitation recordings

`aws-recordings.json` holds what real Cognito (us-east-1, 2026-11-10) did
with AdminCreateUser invitations on a user pool created with no
SmsConfiguration: which delivery problems fail the call (the user is still
created) and what `MessageAction: RESEND` requires and changes. Recorded
with the AWS CLI; every user pool was deleted afterwards. Unit tests in
`src/service/tests.rs` cite the entries they reproduce.
