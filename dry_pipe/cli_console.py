from prompt_toolkit.application import Application
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.document import Document
from prompt_toolkit.layout.dimension import LayoutDimension as D
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout.containers import HSplit, Window
from prompt_toolkit.layout.layout import Layout
from prompt_toolkit.widgets import TextArea

class ConsoleSession:

    def __init__(self):
        self.pipeline_instance_dir = None
        self.pipeline_generator = None
        self.pipeline_status = None

    def command_start(self, arg):
        self.pipeline_status = "running"

    def command_load_pipeline(self, arg):
        self.pipeline_status = "running"

    def command_stop_pipeline(self, arg):
        self.pipeline_status = "running"

    def command_continue_pipeline(self, arg):
        self.pipeline_status = "running"

class DryPipeConsole:

    def __init__(self):
        self.buffer_max_lines = 1024
        self.buffer_lines = []
        self.session = ConsoleSession()

    def get_statusbar_text(self):
        x = 5
        y = 4356
        return [
            ("class:status", " zaz "),
            (
                "class:status.position",
                f"{x}:{y}",
            ),
            ("class:status", " - Press "),
            ("class:status.key", "Ctrl-C"),
            ("class:status", " to exit, "),
            ("class:status.key", "/"),
            ("class:status", " for searching."),
        ]

    def _build_layout(self):

        output_field = TextArea(style="bg:#000044 #ffffff", text="...")
        input_field = TextArea(
            height=1,
            prompt="~> ",
            style="bg:#000000 #ffffff",
            multiline=False,
            wrap_lines=False
        )

        input_field.accept_handler = lambda buff: self.accept(output_field, buff)

        return Layout(
            HSplit([
                Window(
                    content=FormattedTextControl(lambda: self.get_statusbar_text()),
                    height=D.exact(1),
                    style="class:status",
                ),
                output_field,
                Window(height=1, char="_", style="#004400"),
                input_field
            ]),
            focused_element=input_field
        )

    def accept(self, output_field, buff):

        output = f"eval<{buff.text}>"

        self.buffer_lines.append(output)

        if len(self.buffer_lines) > self.buffer_max_lines:
            self.buffer_lines = self.buffer_lines[-self.buffer_max_lines:]

        new_text = "\n".join(self.buffer_lines)

        output_field.buffer.document = Document(
            text=new_text,
            cursor_position=len(new_text)
        )

    def run(self):

        kb = KeyBindings()

        @kb.add("c-c")
        @kb.add("c-q")
        def _(event):
            "Pressing Ctrl-Q or Ctrl-C will exit the user interface."
            event.app.exit()

        application = Application(
            self._build_layout(),
            key_bindings=kb,
            mouse_support=True,
            full_screen=True,
        )

        application.run()


if __name__ == "__main__":
    c = DryPipeConsole()
    c.run()
