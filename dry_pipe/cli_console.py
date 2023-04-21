from prompt_toolkit.application import Application
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.document import Document
from prompt_toolkit.layout.dimension import LayoutDimension as D
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout.containers import HSplit, Window
from prompt_toolkit.layout.layout import Layout
from prompt_toolkit.widgets import TextArea
from prompt_toolkit.completion import Completer, Completion, ThreadedCompleter
import time

class ConsoleSession:

    def __init__(self):
        self.pipeline_instance_dir = None
        self.pipeline_generator = None
        self.pipeline_status = None
        self.trace = ""

    def command_start(self, arg):
        self.pipeline_status = "running"
        return f"{arg} started"

    def command_load_pipeline(self, arg):
        self.pipeline_status = "zz:pop"
        return f"{arg} loaded"

    def command_stop_pipeline(self):
        self.pipeline_status = "stopped"
        return f"{self.pipeline_generator} stopped"

    def command_continue_pipeline(self, arg):
        self.pipeline_status = "running"
        return f"{arg} restarted without generator"

    def all_commands(self):
        return [
            a[8:]
            for a in dir(self)
            if a.startswith("command_")
        ]

    def invoke_command(self, buffer):
        cmd = buffer.split()
        command_name = cmd[0]
        args = cmd[1:]
        c = self.__getattribute__(f"command_{command_name}")
        res = c(*args)
        return res


class DryPipeConsole:

    def __init__(self):
        self.buffer_max_lines = 1024
        self.buffer_lines = []
        self.session = ConsoleSession()

    def get_statusbar_text(self):
        return [
            ("class:status", " zaz "),
            ("class:status", self.session.trace),
            ("class:status", " - Press "),
            ("class:status.key", "Ctrl-C"),
            ("class:status", " to exit, ")
        ]

    def _build_layout(self):

        commands = self.session.all_commands()
        console = self
        class SlowCompleter(Completer):

            def __init__(self):
                self.loading = 0

            def get_completions(self, document, complete_event):
                # Keep count of how many completion generators are running.
                self.loading += 1
                word_before_cursor = document.get_word_before_cursor()

                console.trace = f"{complete_event.completion_requested}"

                try:
                    for word in commands:
                        if word.startswith(word_before_cursor):
                            #time.sleep(0.2)  # Simulate slowness.
                            yield Completion(word, -len(word_before_cursor))

                finally:
                    # We use try/finally because this generator can be closed if the
                    # input text changes before all completions are generated.
                    self.loading -= 1

        output_field = TextArea(style="bg:#000044 #ffffff", text="...")
        input_field = TextArea(
            height=1,
            prompt="~> ",
            style="bg:#000000 #ffffff",
            multiline=False,
            wrap_lines=False,
            completer=ThreadedCompleter(SlowCompleter())
        )

        input_field.accept_handler = lambda buff: self.accept(output_field, buff)

        top_line = Window(
            content=FormattedTextControl(lambda: self.get_statusbar_text()),
            height=D.exact(1),
            style="class:status",
        )

        return Layout(
            HSplit([
                top_line,
                output_field,
                Window(height=1, char="_", style="#004400"),
                input_field
            ]),
            focused_element=input_field
        )

    def accept(self, output_field, buff):

        output = self.session.invoke_command(buff.text)

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

        def t():
            while True:
                application.invalidate()
                time.sleep(0.1)

        application.run()

        #application.create_background_task()


if __name__ == "__main__":
    c = DryPipeConsole()
    c.run()
