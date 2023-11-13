package mig003

const emptyManifest string = `# yaml-language-server: $schema=https://www.topaz.sh/schema/manifest.json
---

model:
  version: 3

### object type definitions ###
types:
`

const baseManifest string = `# yaml-language-server: $schema=https://www.topaz.sh/schema/manifest.json
---

model:
  version: 3

### object type definitions ###
types:
  ### display_name: User ###
  user:
    relations:
      ### display_name: user#manager ###
      manager: user

  ### display_name: Identity ###
  identity:
    relations:
      ### display_name: identity#identifier ###
      identifier: user

  ### display_name: Group ###
  group:
    relations:
      ### display_name: group#member ###
      member: user
`
