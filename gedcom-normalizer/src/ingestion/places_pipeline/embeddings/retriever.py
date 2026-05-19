class Retriever:

    def __init__(self, index_path, meta_path, model):

        self.index = faiss.read_index(index_path)

        with open(meta_path, "rb") as f:
            self.meta = pickle.load(f)

        self.model = model

    def search(self, query, k=20):

        emb = self.model.encode([query]).astype("float32")

        _, idxs = self.index.search(emb, k)

        return [self.meta[i] for i in idxs[0]]