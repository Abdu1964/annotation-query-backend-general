from mork.mork_client import MORK, ManagedMORK
import os
import glob
from pathlib import Path
from hyperon import MeTTa, SymbolAtom, ExpressionAtom, GroundedAtom
from metta.metta_seralizer import metta_seralizer

class MorkQueryGenerator:
    def __init__(self, dataset_path):
        self.server = self.connect()
        self.metta = MeTTa()
        self.clear_space()
        self.load_dataset(dataset_path)

    def connect(self):
        server = ManagedMORK.connect(url='http://127.0.0.1:8000')
        return server

    def clear_space(self):
        self.server.clear()

    def load_dataset(self, path):
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")
        paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
        if not paths:
            raise ValueError(f"No .metta files found in dataset path '{path}'.")
        with self.server.work_at("annotation") as annotation:
            for path in paths:
                path = Path(path)
                file_url = path.resolve().as_uri()
                try:
                    annotation.sexpr_import_(file_url).block()
                    print("Data loaded successfully")
                except Exception as e:
                    print(f"Error loading data: {e}")
