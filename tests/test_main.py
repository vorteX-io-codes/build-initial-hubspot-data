from build_initial_hubspot_data.main import say_hello_world


def test_say_hello_world() -> None:
    assert say_hello_world() == 'Hello World'
