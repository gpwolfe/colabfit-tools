import markdown
import unittest
from ase import Atoms

from core import ATOMS_NAME_FIELD, ATOMS_LABELS_FIELD
from core.dataset import Dataset

class TestMDConstructor(unittest.TestCase):
    def test_from_html(self):
        with open('tests/files/test.md', 'r') as f:
            dataset = Dataset.from_markdown(
                markdown.markdown(f.read(), extensions=['tables'])
            )
        self.assertTrue(False)

    def test_config_setter(self):
        atoms = []
        for i in range(3):
            atoms.append(Atoms())
            atoms[-1].info[ATOMS_NAME_FIELD] = i

        dataset = Dataset(name='test')
        dataset.configurations = atoms

        self.assertEqual(
            ['0', '1', '2'],
            [a.info[ATOMS_NAME_FIELD] for a in dataset.configurations]
        )

        del dataset.configurations[1]

        self.assertEqual(
            ['0', '2'],
            [a.info[ATOMS_NAME_FIELD] for a in dataset.configurations]
        )

        new_atoms = Atoms()
        new_atoms.info[ATOMS_NAME_FIELD] = 4
        dataset.configurations += [new_atoms]

        self.assertEqual(
            ['0', '2', '4'],
            [a.info[ATOMS_NAME_FIELD] for a in dataset.configurations]
        )

    def test_co_label_refresher(self):

        dataset = Dataset(name='test')
        dataset.configurations = [Atoms() for _ in range(10)]

        dataset.co_label_regexes = {
            '[0-4]': {'0_to_4'},
            '[5-9]': {'5_to_9'},
        }

        # Make sure dummy name generator is working properly, and label refresh
        for ai, atoms in enumerate(dataset.configurations):
            self.assertEqual(atoms.info[ATOMS_NAME_FIELD], f"test_{ai}")
            if ai < 5:
                self.assertSetEqual(atoms.info[ATOMS_LABELS_FIELD], {'0_to_4'})
            elif ai >= 5:
                self.assertSetEqual(atoms.info[ATOMS_LABELS_FIELD], {'5_to_9'})


        # Make sure that a new CO gets its labels updated
        new_atoms = Atoms()
        new_atoms.info[ATOMS_NAME_FIELD] = '4'
        dataset.configurations += [new_atoms]

        self.assertSetEqual(
            dataset.configurations[-1].info[ATOMS_LABELS_FIELD], {'0_to_4'}
        )

    def test_cs_refresher(self):

        dataset = Dataset(name='test')
        dataset.configurations = [Atoms() for _ in range(10)]

        dataset.cs_regexes = {
            'default': 'default',
            '[0-4]': '0_to_4',
            '[5-9]': '5_to_9',
        }

        for cs in dataset.configuration_sets:
            for atoms in cs.configurations:
                if int(atoms.info[ATOMS_NAME_FIELD][-1]) < 5:
                    self.assertEqual(cs.description, '0_to_4')
                else:
                    self.assertEqual(cs.description, '5_to_9')

    
    def test_labels_updated_everywhere(self):

        dataset = Dataset(name='test')
        dataset.configurations = [Atoms() for _ in range(10)]

        dataset.cs_regexes = {
            'default': 'default',
            '[0-4]': '0_to_4',
            '[5-9]': '5_to_9',
        }

        # Ensure labels are initially empty
        for cs in dataset.configuration_sets:
            for atoms in cs.configurations:
                self.assertSetEqual(set(), atoms.info[ATOMS_LABELS_FIELD])

        # Make sure they're re-written correctly
        dataset.co_label_regexes = {
            '[0-4]': {'0_to_4'},
            '[5-9]': {'5_to_9'},
        }

        for cs in dataset.configuration_sets:
            for atoms in cs.configurations:
                if int(atoms.info[ATOMS_NAME_FIELD][-1]) < 5:
                    self.assertSetEqual(
                        {'0_to_4'}, atoms.info[ATOMS_LABELS_FIELD]
                    )
                else:
                    self.assertSetEqual(
                        {'5_to_9'}, atoms.info[ATOMS_LABELS_FIELD]
                    )

        # Make sure they're added to correctly
        dataset.co_label_regexes['[0-9]'] = {'new_label'}

        for cs in dataset.configuration_sets:
            for atoms in cs.configurations:
                if int(atoms.info[ATOMS_NAME_FIELD][-1]) < 5:
                    self.assertSetEqual(
                        {'0_to_4', 'new_label'}, atoms.info[ATOMS_LABELS_FIELD]
                    )
                else:
                    self.assertSetEqual(
                        {'5_to_9', 'new_label'}, atoms.info[ATOMS_LABELS_FIELD]
                    )

        # And also removed properly
        dataset.co_label_regexes.pop('[0-4]')

        for cs in dataset.configuration_sets:
            for atoms in cs.configurations:
                if int(atoms.info[ATOMS_NAME_FIELD][-1]) < 5:
                    self.assertSetEqual(
                        {'new_label'}, atoms.info[ATOMS_LABELS_FIELD]
                    )
                else:
                    self.assertSetEqual(
                        {'5_to_9', 'new_label'}, atoms.info[ATOMS_LABELS_FIELD]
                    )


