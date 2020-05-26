from TFRecords.GenerateTFRecord import GenerateTFRecord


class Application:

    def run(self, data, label, output_file_path):
        generator = GenerateTFRecord(data, label, output_file_path)
        generator.run()

