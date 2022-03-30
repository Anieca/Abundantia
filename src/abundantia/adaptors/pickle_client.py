import pickle


class PickleClient:
    @classmethod
    def dump(self, filename, obj, **kwargs):
        with open(filename, "wb") as f:
            pickle.dump(obj, f, **kwargs)

    @classmethod
    def load(self, filename):
        with open(filename, "rb") as f:
            return pickle.load(f)
