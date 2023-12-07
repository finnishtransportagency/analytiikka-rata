from aws_cdk import (
    Tags
)

"""
Lisää tagit
"""
def add_tags(item, tags: dict, project_tag: str = None):
    if tags:
        for tag in tags:
            if tag != None and tag != "" and tags[tag] != None and tags[tag] != "":
                #print(f"'{tag}' = '{tags[tag]}'")
                Tags.of(item).add(tag, tags[tag], apply_to_launched_instances = True, priority = 300)
    if project_tag != None and project_tag != "":
        Tags.of(item).add("Project", project_tag, apply_to_launched_instances = True, priority = 300)

