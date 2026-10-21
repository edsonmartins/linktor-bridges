## Por quê

<!-- Qual problema este PR resolve? Link da issue, se houver. -->

## O que muda

<!-- O que foi alterado, em poucas linhas. Desktop, Android, protocolo? -->

## Como testei

<!-- Comandos rodados, device/SO usado, número de teste, prints. -->

## Checklist

- [ ] Commits assinados com `git commit -s` (DCO)
- [ ] Testes adicionados ou atualizados
- [ ] `gofmt`, `go vet ./...`, `go test ./...` e `go build ./cmd/bridge-desktop` passam (em `go/`)
- [ ] `./scripts/build-aar.sh` e `./gradlew -p android :app:assembleDebug` passam (se mexeu no Android ou no `wacore`)
- [ ] `docs/PROTOCOL.md` atualizado (se mudou algum frame do protocolo)
