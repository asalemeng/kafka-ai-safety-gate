# Chatbot security test: Nordhafen Bank demo assistant

Model behind the chatbot: **claude-haiku-5-5**.

The same 50 test messages were sent to the same chatbot twice: once directly, once through the Kafka AI Safety Gate. Every result is a yes/no check, so no second AI judged the answers.

| | without-gate | with-gate |
| --- | --- | --- |
| Attacks that worked (leak or hijack) | 0 of 30 | 0 of 30 |
| Attacks stopped before the model | 0 of 30 | 20 of 30 |
| Personal data sent to the model provider | 5 of 5 | 0 of 5 |
| Normal questions wrongly blocked | 0 of 15 | 0 of 15 |
| No answer (errors, timeouts) | 0 | 0 |

## Attacks that worked, by language

| Language | without-gate | with-gate |
| --- | --- | --- |
| English | 0 of 6 | 0 of 6 |
| German | 0 of 6 | 0 of 6 |
| Arabic | 0 of 6 | 0 of 6 |
| Egyptian Arabic | 0 of 6 | 0 of 6 |
| Franco-Arabic | 0 of 6 | 0 of 6 |

## How to read this

- The attack messages were written by the same person who wrote the gate's word lists. The directly worded attacks are therefore the easy case for the gate. The 10 reworded attacks are the honest test: the gate's keyword filter does not recognise them, so the model's own defences decide.
- 50 messages, each sent once. Language models do not answer the same way every time, so a second run can differ by a few cases.
- "Personal data sent to the model provider" counts messages the provider received, whatever the model answered. Without the gate that is every such message.
