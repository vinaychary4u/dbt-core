import pathlib

from dbt.cli.main import dbtRunner

profile_with_jinjaesque_password = """test:
  outputs:
    default:
      dbname: my_db
      host: localhost
      password: no{{jinja{%re{#ndering
      port: 12345
      schema: dummy
      threads: 4
      type: postgres
      user: peter.webb
  target: default
"""


class TestProfileParsing:
    def test_no_jinja_for_password(self, project, profiles_root):
        with open(pathlib.Path(profiles_root, "profiles.yml"), "w") as profiles_yml:
            profiles_yml.write(profile_with_jinjaesque_password)

        events = []
        result = dbtRunner(callbacks=[events.append]).invoke(["parse"])
        assert result.success  # for now, at least, just ensure success

        for e in events:
            assert "no{{jinja{%re{#ndering" not in e.info.msg
