# Contributing to Paintress

Thanks for wanting to help. Bug reports, measurements from your own bench,
fixes to the docs and code are all welcome.

## License of contributions

Paintress is under the GNU General Public License, version 3 or later
(`GPL-3.0-or-later`). What you contribute is under the same license.

## Sign your commits (DCO)

Paintress uses the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO). You keep the copyright on what you write. Signing off only says that
you have the right to send it under the project's license.

Every commit needs a `Signed-off-by` line. Git adds it for you with `-s`:

```sh
git commit -s -m "Fix the thing"
```

which appends:

```
Signed-off-by: Your Name <you@example.com>
```

Use your real name and an email that works. A commit without it can't be
merged. If you forgot, `git commit --amend -s` fixes the last commit, and
`git rebase --signoff <base>` fixes a whole branch.

This is the full text you agree to:

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## New source files

Start every new source file with these two lines, as comments in that
language:

```
SPDX-FileCopyrightText: 2026 paintress-team
SPDX-License-Identifier: GPL-3.0-or-later
```

A file copied from another project keeps its own license and notice. Say
where it came from in the pull request.
