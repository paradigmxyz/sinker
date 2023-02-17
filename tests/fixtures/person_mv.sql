select id,
       json_build_object(
               'name', "name") as "person"
from "person"
