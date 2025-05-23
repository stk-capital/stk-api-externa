# Plano de Implementação – Normalização de Companies e Eliminação de Duplicatas

> **Objetivo**: Padronizar campos `name` e `ticker` na coleção `companies` (formato "Company Example" e `UPPER`), impedir novas duplicatas e resolver condição de corrida **sem** alterar o schema (sem novos campos), seguindo os *edition-principles*.

---

## 1. Contexto
* Coleção `companies` contém ~7 400 documentos e centenas de duplicatas.
* Não existe índice único; inserções paralelas + capitalização/ticker inconsistentes geram registros repetidos.
* Fluxos críticos: `intruments_to_companies_ids_parallel` e derivados.

## 2. Requisitos
1. **Intervenção mínima**: nenhum novo campo; manter APIs.
2. **Nome padrão**: Title Case preservando siglas ("XP Inc" → "XP Inc").
3. **Ticker padrão**: `upper().strip()`; se `None`/"", usar `"PRIVATE"`.
4. **Corrida resolvida**: uso de índice único + upsert atômico.
5. **Zero downtime**: scripts idempotentes, índice em *background*.

## 3. Fases de Implementação

### Fase 0 – Validação (já feita)
* Sem `.hint()`/`.collation()` conflitantes.
* Modelos Pydantic livres de restrições rígidas.

### Fase 1 – Script de Normalização
`scripts/normalize_company_fields.py`
1. Cursor batch (1 000 docs).
2. Para cada doc: calcular `new_name`, `new_ticker` usando helpers `normalize_name`/`normalize_ticker`.
3. Se mudou → `update_one({_id}, {$set:{name:new_name, ticker:new_ticker}})`.
4. `--dry-run` default; `--execute` aplica mudanças.

### Fase 2 – Índice Único
```js
// mongo shell
 db.companies.createIndex(
   { name: 1, ticker: 1 },
   { unique: true, background: true, collation: { locale: 'en', strength: 2 } }
 )
```
* `strength:2` ⇒ case-insensitive (resolve "XP" vs "Xp").
* Criar **após** normalização para reduzir conflitos.

### Fase 3 – Refatoração do Fluxo de Criação
Arquivos a tocar:
* `util/companies_utils.py` (`intruments_to_companies_ids_parallel`)
* Eventuais `get_or_create_company` helpers.

Alterações:
1. Importar e usar `normalize_name`/`normalize_ticker` antes de consultas.
2. Substituir `insert_many` por loop de `find_one_and_update(filter, {$setOnInsert: {...}}, upsert=True, return_document=AFTER)`.
3. Capturar eventuais `DuplicateKeyError` (fallback).
4. Garantir retorno do `_id` único.

### Fase 4 – Testes de Regressão
* `tests/test_data_integrity_duplicates.py`
  * Acrescentar assert `name == normalize_name(name)`.
  * Re-executar duplicatas ⇒ deve ser **0**.
* Teste concorrente: 20 threads chamando função de criação com mesmo input; assert count==1.

### Fase 5 – Deploy & Monitoramento
1. Pausar ingestion heavy (ou throttling).
2. Executar script de normalização (`--execute`).
3. Criar índice único.
4. Deploy código refatorado.
5. Monitorar logs/alertas (DuplicateKeyError inesperado).
6. Rodar testes de integridade em produção (read-only).

## 4. Rollback
* Índice único pode ser removido: `db.companies.dropIndex('name_1_ticker_1')`.
* Script de normalização é idempotente (pode reverter backup se necessário).
* Código novo opera corretamente mesmo sem índice (só perde proteção).

## 5. Riscos & Mitigações
| Risco | Mitigação |
|------|-----------|
| Conflito ao criar índice (duplicatas remanescentes) | Re-executar `cleanup_duplicates.py` para grupo reportado. |
| Timeout em updates massivos | Script usa batch e `retryWrites`. |
| Falhas de outras coleções referenciando ticker/ name | Não há, pois `_id` permanece o mesmo. |

## 6. Timeline Sugerida
1. **T-0** – Merge helpers + script (dev).
2. **T-1** – Dry-run script em staging.
3. **T-2** – Execução real em produção (<15 min).
4. **T-2+5min** – Criar índice background.
5. **T-2+15min** – Deploy código.
6. **T-2+30min** – Executar testes de integridade.

---
*Documento gerado automaticamente conforme princípios de edição: mínima intervenção, reaproveito de partes existentes e sem quebra de funcionalidades.* 