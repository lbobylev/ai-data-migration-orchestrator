from langchain_core.callbacks import BaseCallbackHandler

class ToolLogger(BaseCallbackHandler):
    def on_tool_start(self, tool, input_str, **kwargs):
        tool_name = tool["name"]
        print(f"[TOOL START] {tool_name} with input: {input_str}")

    def on_tool_end(self, output, **kwargs):
        # print(f"[TOOL END] Output: {output}")
        print(f"[TOOL END]")

    # def on_llm_start(self, serialized, prompts, **kwargs):
    #     print(f"[LLM START] Prompts:\n{prompts}")
    #
    # def on_llm_end(self, response, **kwargs):
    #     print(f"[LLM END]")# Response:\n{response}")
