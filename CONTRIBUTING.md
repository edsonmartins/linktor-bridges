# Como contribuir com o Linktor Bridge

Contribuições são bem-vindas: correções, itens do roadmap (mídia, interativos, agent no Android, instaladores), testes, documentação. Este guia explica como preparar o ambiente, o que um PR precisa ter e como ele é revisado.

## Antes de começar

- Para correções pequenas, abra o PR direto.
- Para mudanças grandes (frame novo no protocolo, mudança no `wacore`, nova plataforma), abra antes uma [issue](https://github.com/edsonmartins/linktor-bridges/issues) descrevendo a proposta. Assim ninguém perde trabalho com algo que não vai entrar.
- O lado servidor do protocolo (`internal/gateway/`, `RemoteAdapter`) vive no [Linktor](https://github.com/edsonmartins/linktor). Mudança no protocolo normalmente precisa de PR nos dois repositórios.
- **Falhas de segurança não vão em issue pública.** Use [Report a vulnerability](https://github.com/edsonmartins/linktor-bridges/security/advisories/new) na aba Security do repositório.

## Preparando o ambiente

1. Faça um fork do repositório e clone o seu fork.
2. Desktop: Go na versão do `go/go.mod`. Siga a seção [Como rodar](README.md#como-rodar) do README.
3. Android (só se for mexer no app ou no `wacore`): Android SDK com `ANDROID_HOME` definido; o `scripts/build-aar.sh` instala o `gomobile` sozinho.
4. Crie uma branch a partir da `main`:

```bash
git switch -c fix/descricao-curta
```

**Use sempre um número de teste.** O `whatsmeow` não é API oficial da Meta e há risco de bloqueio da linha pareada.

## Antes de abrir o PR

```bash
cd go
gofmt -l .          # não deve listar nada
go vet ./...
go test ./...
go build ./cmd/bridge-desktop

# Android (se mexeu em android/ ou go/wacore), a partir da raiz
cd ..
./scripts/build-aar.sh
./gradlew :app:assembleDebug
```

Inclua testes para o que mudou. Correção de bug vem com um teste que falhava antes e passa depois. O `agent` tem testes contra um servidor WS mockado: use-os de modelo para mudanças no protocolo.

### Regras do código

- `go/wacore` é o **único** pacote autorizado a importar `whatsmeow`. O resto da bridge fala com o WhatsApp através dele.
- Mudou um frame do protocolo? Atualize o [`docs/PROTOCOL.md`](docs/PROTOCOL.md) no mesmo PR.
- Nada de sessão, chave ou número real em commits, testes ou logs.

## Commits

Seguimos [Conventional Commits](https://www.conventionalcommits.org/), com a descrição em português:

```
fix(agent): reconexão deixa de reenviar eventos já confirmados
feat(wacore): envio de imagem outbound
docs(protocol): documenta o frame call_command
```

Tipos usados: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`. O escopo entre parênteses é a área do código (`wacore`, `agent`, `eventstore`, `desktop`, `android`, `protocol`...).

No corpo do commit, explique o porquê: o que estava errado e o que a mudança resolve. O diff já mostra o como.

### Assinatura (DCO)

Todo commit de contribuição precisa da linha `Signed-off-by`, que declara que você tem o direito de enviar aquele código sob a licença do projeto, nos termos do [Developer Certificate of Origin](https://developercertificate.org/). Basta commitar com `-s`:

```bash
git commit -s -m "fix(eventstore): evento só sai da fila após o ack"
```

Esqueceu em commits já feitos? Assine todos de uma vez e atualize o PR:

```bash
git rebase --signoff main
git push --force-with-lease
```

Um check chamado **DCO** confere a assinatura em cada PR.

## O PR

- Um assunto por PR. Correções independentes vão em PRs separados.
- Descreva o problema, a solução e como você testou (SO/device usado). O modelo de PR já traz essas seções.
- Mudança visual no dashboard desktop ou no app Android: inclua print ou gif.

No seu primeiro PR, o CI só roda depois que um mantenedor aprovar a execução. É uma proteção padrão do GitHub para contas novas no repositório; a partir do segundo PR ele roda sozinho.

O merge acontece depois do CI verde e da revisão de um mantenedor.

## Licença

O Linktor Bridge é distribuído sob a [Apache License 2.0](LICENSE). Ao contribuir, você concorda que sua contribuição seja licenciada nos mesmos termos.
