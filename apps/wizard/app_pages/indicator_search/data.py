from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin

from apps.wizard.utils.embeddings import Doc


@dataclass
class Indicator(Doc, DataClassJsonMixin):
    variableId: int
    name: str
    description: str
    n_charts: int
    catalogPath: str
    dataset: str | None = None
    popularity: float = 0.0

    def text(self) -> str:
        # Combine the name and description into a single string
        # NOTE: Using both name and description can be sometimes too long, making the embedding less accurate.
        #  One example is query "beer" for which the indicator
        #     "Alcohol, recorded per capita (15+) consumption (in litres of pure alcohol) - Beverage types: Beer"
        #  has lower similarity than indicator "Coffee".
        #  Append description only if the name is too short helps a bit. An alternative would be to shorten
        #  it with a summarizer before embedding.
        return self.name + " " + self.description
