class OllamaClient:

    def __init__(self, model):
        self.model = model

    def match(self, prompt):

        response = requests.post(
            "http://localhost:11434/api/chat",
            json={
                "model": self.model,
                "temperature": 0,
                "messages": prompt
            }
        )

        return response.json()["message"]["content"]