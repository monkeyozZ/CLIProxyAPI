# Kiro payload rules in v8

Kiro payload rules now run once on the final Kiro request, after message conversion, thinking instructions, model selection, and history tool placeholders. Configured values are preserved, and filtered fields are not recreated before transmission. This applies to both streaming and non-streaming requests.

Keep `protocol: claude` in model selectors: Kiro still uses the Claude translation pipeline. Parameter paths now refer to the native Kiro body, such as `conversationState.currentMessage.userInputMessage.content` and `profileArn`.

There is no `executor` selector in the payload configuration. Use an `exist` condition on a Kiro-specific field to avoid matching ordinary Claude requests. You can also narrow `name` to a Kiro-specific model name or alias.

```yaml
config-version: 8
requests:
  payload:
    override:
      - models:
          - name: "*"
            protocol: claude
            exist:
              - conversationState.currentMessage.userInputMessage
        params:
          conversationState.currentMessage.userInputMessage.content: "Use this prompt."
    filter:
      - models:
          - name: "*"
            protocol: claude
            exist:
              - conversationState.currentMessage.userInputMessage
        params:
          - conversationState.currentMessage.userInputMessage.userInputMessageContext.tools
```

Move rules that previously addressed the intermediate Claude payload, including `messages`, `tools`, `system`, or `thinking`, to the corresponding final Kiro fields. For example, the tool list is now `conversationState.currentMessage.userInputMessage.userInputMessageContext.tools`. Kiro's built-in message, tool, and thinking conversion remains in place and completes before these rules run. Claude parameter paths are not automatically mapped to Kiro paths.
