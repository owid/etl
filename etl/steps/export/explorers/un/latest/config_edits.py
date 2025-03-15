class ConfigEditor:
    @classmethod
    def edit_views_pop(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator != "population_density":
                if sex == "all":
                    title = "Population"
                else:
                    title = f"{sex.title()} population"

                if age != "all":
                    if age == "0":
                        title += " of children under the age of 1"
                    elif age == "0-4":
                        title += " of children under the age of 5"
                    elif age == "0-14":
                        title += " of children under the age of 15"
                    elif age == "0-24":
                        title += " under the age of 25"
                    elif "-" in age:
                        title += f" aged {age.replace('-', ' to ')} years"
                    elif "+" in age:
                        title += f" older than {age.replace('+', '')} years"
                    else:
                        title += f" at age {age} years"

                if indicator == "population_change":
                    title = f"Annual change in {title.lower()}"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

    @classmethod
    def edit_views_fr(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            age = v.dimensions["age"]
            if indicator == "fertility_rate":
                if age == "all":
                    title = "Fertility rate: children per woman"
                else:
                    title = f"Fertility rate from mothers aged {age.capitalize().replace('-', ' to ')}"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

    @classmethod
    def edit_views_b(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            age = v.dimensions["age"]
            if indicator == "births":
                if age == "all":
                    title = "Births"
                else:
                    title = f"Births from mothers aged {age.capitalize().replace('-', ' to ')}"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

    @classmethod
    def edit_views_deaths(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator == "deaths":
                if sex == "all":
                    title = "Deaths"
                else:
                    title = f"{sex.title()} deaths"

                if age != "all":
                    if age == "0":
                        title += " of children under the age of 1"
                    elif age == "0-4":
                        title += " of children under the age of 5"
                    elif age == "0-14":
                        title += " of children under the age of 15"
                    elif age == "0-24":
                        title += " under the age of 25"
                    elif "-" in age:
                        title += f" aged {age.replace('-', ' to ')} years"
                    elif "+" in age:
                        title += f" older than {age.replace('+', '')} years"
                    else:
                        title += f" at age {age} years"

                if v.config is None:
                    v.config = {"title": title}
                else:
                    v.config["title"] = title

    @classmethod
    def edit_views_le(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator == "life_expectancy":
                if sex == "all":
                    title = "Life expectancy"
                else:
                    title = f"{sex.title()} life expectancy"

                if age == 0:
                    title += " at birth"
                else:
                    title += f" at age {age}"

                if v.config is None:
                    v.config = {
                        "title": title,
                        "yAxisMin": int(age),
                    }
                else:
                    v.config["title"] = title
                    v.config["yAxisMin"] = int(age)

    @classmethod
    def edit_views_sr(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            age = v.dimensions["age"]
            if indicator == "sex_ratio":
                title = "Sex ratio"

                if age == "0":
                    title += " at birth"
                elif age == "100+":
                    title += " at age 100 and over"
                elif age != "all":
                    title += f" at age {age}"

                if v.config is None:
                    v.config = {
                        "title": title,
                    }
                else:
                    v.config["title"] = title

    @classmethod
    def edit_views_dr(cls, explorer):
        for v in explorer.views:
            indicator = v.dimensions["indicator"]
            sex = v.dimensions["sex"]
            age = v.dimensions["age"]
            if indicator == "dependency_ratio":
                sex_label = sex + " " if sex != "all" else ""
                if age == "total":
                    title = f"total {sex_label}dependency ratio"
                elif age == "youth":
                    title = f"{sex_label}youth dependency ratio"
                elif age == "old":
                    title = f"{sex_label}old-age dependency ratio"
                else:
                    raise ValueError(f"Unknown age: {age}")

                if v.config is None:
                    v.config = {
                        "title": title.capitalize(),
                    }
                else:
                    v.config["title"] = title.capitalize()
