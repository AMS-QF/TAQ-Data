import yaml


# remove all dependencies from the environment.yml file
def remove_specific_dependencies(env_file, dependencies_to_remove):

    # remove the dashes
    with open(env_file, "r") as stream:
        data_loaded = yaml.safe_load(stream)

    dependecy_list = []

    # remove the dependencies
    for dep_name in dependencies_to_remove:
        for full_dep in data_loaded["dependencies"]:
            if dep_name in full_dep:
                dependecy_list.append(full_dep)
                data_loaded["dependencies"].remove(full_dep)

    # add the remaining dependecies back
    with open(env_file, "w") as stream:
        yaml.dump(data_loaded, stream, default_flow_style=False)

    return


# python remove_dependencies.py
if __name__ == "__main__":

    dependecies_to_remove = [
        "libstdcxx-ng",
        "nspr",
        "libedit",
        "ncurses",
        "readline",
        "dbus",
        "libgcc-ng",
        "libuuid",
        "ld_impl_linux-64",
        "libxkbcommon",
        "nss",
        "libgomp",
        "_openmp_mutex",
    ]

    remove_specific_dependencies("../environment.yml", dependecies_to_remove)
