class GeoMatcher:

    def __init__(self, retriever, llm):

        self.retriever = retriever
        self.llm = llm

    def match(self, place_clean):

        # 1. retrieve
        candidates = self.retriever.search(place_clean, k=20)

        # 2. build prompt
        prompt = self.build_prompt(place_clean, candidates)

        # 3. LLM decision
        result = self.llm.match(prompt)

        return result