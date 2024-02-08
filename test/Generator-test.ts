import * as Path from 'path';

import { runConfig as runEnhancer } from 'ldbc-snb-enhancer';
import { runConfig as runValidationGenerator } from 'ldbc-snb-validation-generator';
import { runConfig as runFragmenter } from 'rdf-dataset-fragmenter';
import { walkSolidPods } from 'shape-trees-in-solidbench-generator';
import { runConfig as runQueryInstantiator } from 'sparql-query-parameter-instantiator';

import { Generator } from '../lib/Generator';

let files: Record<string, string> = {};
let filesOut: Record<string, string> = {};
let filesDeleted: Record<string, boolean> = {};
let dirsOut: Record<string, boolean> = {};
let fileExist = true;

jest.mock('fs', () => ({
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  ...<any>jest.requireActual('fs'),
  promises: {
    async readFile(filePath: string) {
      if (filePath in files) {
        return files[filePath];
      }
      throw new Error(`Unknown file in Generator tests: ${filePath}`);
    },
    async writeFile(filePath: string, contents: string) {
      filesOut[filePath] = contents;
    },
    async unlink(filePath: string) {
      filesDeleted[filePath] = true;
    },
    async mkdir(dirPath: string) {
      dirsOut[dirPath] = true;
    },
    async stat(filePath: string) {
      if (filePath in files) {
        return files[filePath];
      }
      throw new Error(`Unknown file in Generator tests: ${filePath}`);
    },
    async readdir(): Promise<string[]> {
      return [ 'abc' ];
    },
  },
  existsSync(_path: string): boolean {
    return fileExist;
  },
}));

jest.mock('shape-trees-in-solidbench-generator');

let container: any = {};
let followProgress: any;
jest.mock('dockerode', () => jest.fn().mockImplementation(() => ({
  createContainer: jest.fn(() => container),
  pull: jest.fn(),
  modem: {
    followProgress,
  },
})));

jest.mock('ldbc-snb-enhancer', () => ({
  runConfig: jest.fn(),
}));

jest.mock('rdf-dataset-fragmenter', () => ({
  runConfig: jest.fn(),
}));

jest.mock('sparql-query-parameter-instantiator', () => ({
  runConfig: jest.fn(),
}));

jest.mock('ldbc-snb-validation-generator', () => ({
  runConfig: jest.fn(),
}));

jest.spyOn(process.stdout, 'write').mockImplementation();
jest.spyOn(process, 'chdir').mockImplementation();

jest.mock('https', () => ({
  request: jest.fn((_param, cb) => {
    cb({
      on: jest.fn(() => ({
        pipe: jest.fn(() => ({
          on: jest.fn(() => ({
            on: jest.fn((name, cbInner) => {
              cbInner();
            }),
          })),
        })),
      })),
    });
    return { end: jest.fn() };
  }),
}));

describe('Generator', () => {
  let generator: Generator;
  let mainModulePath: string;

  beforeEach(() => {
    files = {
      [Path.join(__dirname, '../templates/params.ini')]: 'BLA SCALE BLA',
    };
    filesOut = {};
    filesDeleted = {};
    dirsOut = {};
    container = {
      start: jest.fn(),
      kill: jest.fn(),
      remove: jest.fn(),
      attach: jest.fn(() => ({
        pipe: jest.fn(),
        resume: jest.fn(),
        on: jest.fn((evt, cb) => {
          if (evt === 'end') {
            cb();
          }
        }),
      })),
    };
    fileExist = true;
    mainModulePath = Path.join(__dirname, '..');
    generator = new Generator({
      cwd: 'CWD',
      verbose: true,
      overwrite: true,
      scale: '0.1',
      enhancementConfig: 'enhancementConfig',
      fragmentConfig: 'fragmentConfig',
      enhancementFragmentConfig: 'enhancementFragmentConfig',
      queryConfig: 'queryConfig',
      validationParams: 'validationParams',
      validationConfig: 'validationConfig',
      hadoopMemory: '4G',
    });
    followProgress = jest.fn((buildStream: any, cb: any) => {
      cb();
    });
    jest.clearAllMocks();
  });

  describe('generateSnbDataset', () => {
    it('for a non-existing params.ini template', async() => {
      files = {};

      await expect(generator.generateSnbDataset()).rejects.toThrow('templates/params.ini');
    });

    it('for a valid state', async() => {
      await generator.generateSnbDataset();

      expect(filesOut[Path.join('CWD', 'params.ini')]).toEqual('BLA 0.1 BLA');
      expect(filesDeleted[Path.join('CWD', 'params.ini')]).toEqual(true);
      expect(container.start).toHaveBeenCalled();
      expect(container.attach).toHaveBeenCalled();
      expect(container.remove).toHaveBeenCalled();
      expect(container.kill).not.toHaveBeenCalled();
    });

    it('for a valid state in non-verbose mode', async() => {
      generator = new Generator({
        cwd: 'CWD',
        verbose: false,
        overwrite: true,
        scale: '0.1',
        enhancementConfig: 'enhancementConfig',
        fragmentConfig: 'fragmentConfig',
        enhancementFragmentConfig: 'enhancementFragmentConfig',
        queryConfig: 'queryConfig',
        validationParams: 'validationParams',
        validationConfig: 'validationConfig',
        hadoopMemory: '4G',
      });

      await generator.generateSnbDataset();

      expect(filesOut[Path.join('CWD', 'params.ini')]).toEqual('BLA 0.1 BLA');
      expect(filesDeleted[Path.join('CWD', 'params.ini')]).toEqual(true);
      expect(container.start).toHaveBeenCalled();
      expect(container.attach).toHaveBeenCalled();
      expect(container.remove).toHaveBeenCalled();
      expect(container.kill).not.toHaveBeenCalled();
    });

    it('when interrupted via SIGINT', async() => {
      let onError: any;
      jest.spyOn(process, 'on').mockImplementation(<any>((evt: any, cb: any) => {
        if (evt === 'SIGINT') {
          // eslint-disable-next-line @typescript-eslint/no-implied-eval
          setImmediate(cb);
          setImmediate(() => onError(new Error('container killed')));
        }
      }));
      container.attach = jest.fn(() => ({
        pipe: jest.fn(),
        resume: jest.fn(),
        on: jest.fn((evt, cb) => { // Mock to keep container running infinitely
          if (evt === 'error') {
            onError = cb;
          }
        }),
      }));

      await expect(generator.generateSnbDataset()).rejects.toThrow('container killed');

      expect(filesOut[Path.join('CWD', 'params.ini')]).toEqual('BLA 0.1 BLA');
      expect(filesDeleted[Path.join('CWD', 'params.ini')]).toEqual(true);
      expect(container.kill).toHaveBeenCalled();
    });

    it('when interrupted via SIGINT after container was already ended', async() => {
      let sigintCb: any;
      const sigintCalled = new Promise<void>(resolve => {
        jest.spyOn(process, 'on').mockImplementation(<any>((evt: any, cb: any) => {
          if (evt === 'SIGINT') {
            sigintCb = () => {
              cb();
              resolve();
            };
          }
        }));
      });
      container.attach = jest.fn(() => ({
        pipe: jest.fn(),
        resume: jest.fn(),
        on: jest.fn((evt, cb) => { // Mock to keep container running infinitely
          if (evt === 'end') {
            cb();
            // eslint-disable-next-line @typescript-eslint/no-implied-eval
            setImmediate(sigintCb);
          }
        }),
      }));

      await generator.generateSnbDataset();
      await sigintCalled;

      expect(filesOut[Path.join('CWD', 'params.ini')]).toEqual('BLA 0.1 BLA');
      expect(filesDeleted[Path.join('CWD', 'params.ini')]).toEqual(true);
      expect(container.remove).toHaveBeenCalled();
      expect(container.kill).not.toHaveBeenCalled();
    });

    it('throws for an image pull failure', async() => {
      followProgress = jest.fn((buildStream: any, cb: any) => {
        cb(new Error('FAIL IMAGE PULL'));
      });

      await expect(generator.generateSnbDataset()).rejects.toThrow('FAIL IMAGE PULL');
    });
  });

  describe('enhanceSnbDataset', () => {
    it('should run the enhancer', async() => {
      await generator.enhanceSnbDataset();

      expect(dirsOut[Path.join('CWD', 'out-enhanced')]).toBeTruthy();
      expect(runEnhancer).toHaveBeenCalledWith('enhancementConfig', { mainModulePath });
    });
  });

  describe('fragmentSnbDataset', () => {
    it('should run the fragmenter twice', async() => {
      await generator.fragmentSnbDataset();

      expect(runFragmenter).toHaveBeenCalledWith('fragmentConfig', { mainModulePath });
      expect(runFragmenter).toHaveBeenCalledWith('enhancementFragmentConfig', { mainModulePath });
    });
  });

  describe('instantiateQueries', () => {
    it('should run the instantiator', async() => {
      await generator.instantiateQueries();

      expect(dirsOut[Path.join('CWD', 'out-queries')]).toBeTruthy();
      expect(runQueryInstantiator)
        .toHaveBeenCalledWith('queryConfig', { mainModulePath }, { variables: expect.anything() });
    });
  });

  describe('generateValidation', () => {
    it('should run the validation generator', async() => {
      await generator.generateValidation();

      expect(dirsOut[Path.join('CWD', 'out-validate')]).toBeTruthy();
      expect(runValidationGenerator)
        .toHaveBeenCalledWith('validationConfig', { mainModulePath }, { variables: expect.anything() });
    });
  });

  describe('generateShapeTree', () => {
    describe('getShapeTreeGeneratorInformation', () => {
      it('should return the fragment path when a valid path is provided in the fragment config', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
            {
              "transformers": [
                {
                  "@type": "QuadTransformerBlankToNamed",
                  "searchRegex": "^(b[0-9]*_tagclass)",
                  "replacementString": "http://localhost:3000/www.ldbc.eu/tagclass/$1"
                },
                {
                  "@type": "QuadTransformerReplaceIri",
                  "searchRegex": "^http://www.ldbc.eu",
                  "replacementString": "http://localhost:3000/www.ldbc.eu"
                }
              ],
              "quadSink": {
                "@id": "urn:rdf-dataset-fragmenter:sink:default",
                  "@type": "QuadSinkComposite",
                  "sinks": [
                    {
                      "@type": "QuadSinkFile",
                      "log": true,
                      "outputFormat": "application/n-quads",
                      "fileExtension": ".nq",
                      "iriToPath": {
                        "http://": "out-fragments/http/",
                        "https://": "out-fragments/https/"
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Person$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-persons.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Comment$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-comments.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Post$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-posts.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    }
                  ]
                }
            }
            `);
        });

        const resp = generator.getShapeTreeGeneratorInformation();
        expect(resp).toStrictEqual([ 'out-fragments/https/', 'localhost:3000' ]);
      });

      it('should throw if there is no iriToPath defined in the fragmenter config', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
          {
            "transformers": [
              {
                "@type": "QuadTransformerBlankToNamed",
                "searchRegex": "^(b[0-9]*_tagclass)",
                "replacementString": "http://localhost:3000/www.ldbc.eu/tagclass/$1"
              },
              {
                "@type": "QuadTransformerReplaceIri",
                "searchRegex": "^http://www.ldbc.eu",
                "replacementString": "http://localhost:3000/www.ldbc.eu"
              }
            ],
            "quadSink": {
              "@id": "urn:rdf-dataset-fragmenter:sink:default",
                "@type": "QuadSinkComposite",
                "sinks": [
                  {
                    "@type": "QuadSinkFile",
                    "log": true,
                    "outputFormat": "application/n-quads",
                    "fileExtension": ".nq"
                  },
                  {
                    "@type": "QuadSinkFiltered",
                    "filter": {
                      "@type": "QuadMatcherResourceType",
                      "typeRegex": "vocabulary/Person$",
                      "matchFullResource": false
                    },
                    "sink": {
                      "@type": "QuadSinkCsv",
                      "file": "out-fragments/parameters-persons.csv",
                      "columns": [
                        "subject"
                      ]
                    }
                  },
                  {
                    "@type": "QuadSinkFiltered",
                    "filter": {
                      "@type": "QuadMatcherResourceType",
                      "typeRegex": "vocabulary/Comment$",
                      "matchFullResource": false
                    },
                    "sink": {
                      "@type": "QuadSinkCsv",
                      "file": "out-fragments/parameters-comments.csv",
                      "columns": [
                        "subject"
                      ]
                    }
                  },
                  {
                    "@type": "QuadSinkFiltered",
                    "filter": {
                      "@type": "QuadMatcherResourceType",
                      "typeRegex": "vocabulary/Post$",
                      "matchFullResource": false
                    },
                    "sink": {
                      "@type": "QuadSinkCsv",
                      "file": "out-fragments/parameters-posts.csv",
                      "columns": [
                        "subject"
                      ]
                    }
                  }
                ]
              }
          }
            `);
        });

        expect(() => generator.getShapeTreeGeneratorInformation()).toThrow();
      });

      it('should throw if all the iriToPath in the fragmenter config folders don\'t exist', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
            {
              "transformers": [
                {
                  "@type": "QuadTransformerBlankToNamed",
                  "searchRegex": "^(b[0-9]*_tagclass)",
                  "replacementString": "http://localhost:3000/www.ldbc.eu/tagclass/$1"
                },
                {
                  "@type": "QuadTransformerReplaceIri",
                  "searchRegex": "^http://www.ldbc.eu",
                  "replacementString": "http://localhost:3000/www.ldbc.eu"
                }
              ],
              "quadSink": {
                "@id": "urn:rdf-dataset-fragmenter:sink:default",
                  "@type": "QuadSinkComposite",
                  "sinks": [
                    {
                      "@type": "QuadSinkFile",
                      "log": true,
                      "outputFormat": "application/n-quads",
                      "fileExtension": ".nq",
                      "iriToPath": {
                        "http://": "out-fragments/http/",
                        "https://": "out-fragments/https/"
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Person$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-persons.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Comment$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-comments.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Post$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-posts.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    }
                  ]
                }
            }
            `);
        });
        fileExist = false;
        expect(() => generator.getShapeTreeGeneratorInformation()).toThrow();
      });

      it('should throw if the QuadSinkFile property don\'t exist', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
            {
              "transformers": [
                {
                  "@type": "QuadTransformerBlankToNamed",
                  "searchRegex": "^(b[0-9]*_tagclass)",
                  "replacementString": "http://localhost:3000/www.ldbc.eu/tagclass/$1"
                },
                {
                  "@type": "QuadTransformerReplaceIri",
                  "searchRegex": "^http://www.ldbc.eu",
                  "replacementString": "http://localhost:3000/www.ldbc.eu"
                }
              ],
              "quadSink": {
                "@id": "urn:rdf-dataset-fragmenter:sink:default",
                  "@type": "QuadSinkComposite",
                  "sinks": [
                    {
                      "@type": "boo",
                      "log": true,
                      "outputFormat": "application/n-quads",
                      "fileExtension": ".nq",
                      "iriToPath": {
                        "http://": "out-fragments/http/",
                        "https://": "out-fragments/https/"
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Person$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-persons.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Comment$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-comments.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Post$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-posts.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    }
                  ]
                }
            }
            `);
        });

        expect(() => generator.getShapeTreeGeneratorInformation()).toThrow();
      });

      it('should throw if the fragmenter config have no transformers', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
            {
              "quadSink": {
                "@id": "urn:rdf-dataset-fragmenter:sink:default",
                  "@type": "QuadSinkComposite",
                  "sinks": [
                    {
                      "@type": "QuadSinkFile",
                      "log": true,
                      "outputFormat": "application/n-quads",
                      "fileExtension": ".nq",
                      "iriToPath": {
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Person$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-persons.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Comment$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-comments.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    },
                    {
                      "@type": "QuadSinkFiltered",
                      "filter": {
                        "@type": "QuadMatcherResourceType",
                        "typeRegex": "vocabulary/Post$",
                        "matchFullResource": false
                      },
                      "sink": {
                        "@type": "QuadSinkCsv",
                        "file": "out-fragments/parameters-posts.csv",
                        "columns": [
                          "subject"
                        ]
                      }
                    }
                  ]
                }
            }
            `);
        });

        expect(() => generator.getShapeTreeGeneratorInformation()).toThrow();
      });

      it('should throw if the fragmenter config transformers is empty', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
            {
              "@context": "https://linkedsoftwaredependencies.org/bundles/npm/rdf-dataset-fragmenter/^2.0.0/components/context.jsonld",
              "@id": "urn:rdf-dataset-fragmenter:default",
              "@type": "Fragmenter",
              "quadSource": {
              "@id": "urn:rdf-dataset-fragmenter:source:default",
              "@type": "QuadSourceComposite",
              "sources": [
                {
                  "@type": "QuadSourceFile",
                  "filePath": "out-enhanced/social_network_auxiliary.ttl"
                }
              ]
              },
              "transformers": [
              ],
              "fragmentationStrategy": {
                "@type": "FragmentationStrategyComposite",
                "strategies": [
                  { "@type": "FragmentationStrategySubject" }
                ]
              },
              "quadSink": {
              "@id": "urn:rdf-dataset-fragmenter:sink:default",
              "@type": "QuadSinkFile",
              "log": true,
              "outputFormat": "application/n-quads",
              "fileExtension": ".nq",
                "iriToPath": {
                  "http://": "out-fragments/http/",
                  "https://": "out-fragments/https/"
                }
              }
            }
            
            `);
        });

        expect(() => generator.getShapeTreeGeneratorInformation()).toThrow();
      });

      it('should throw if the fragmenter config first transformer doesn\'t have a replacementString property', () => {
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
            {
              "@context": "https://linkedsoftwaredependencies.org/bundles/npm/rdf-dataset-fragmenter/^2.0.0/components/context.jsonld",
              "@id": "urn:rdf-dataset-fragmenter:default",
              "@type": "Fragmenter",
              "quadSource": {
              "@id": "urn:rdf-dataset-fragmenter:source:default",
              "@type": "QuadSourceComposite",
              "sources": [
                {
                  "@type": "QuadSourceFile",
                  "filePath": "out-enhanced/social_network_auxiliary.ttl"
                }
              ]
              },
              "transformers": [
                {
                  "@type": "QuadTransformerReplaceIri",
                  "searchRegex": "^http://www.ldbc.eu",
                },
                {
                  "@type": "QuadTransformerReplaceIri",
                  "searchRegex": "^http://dbpedia.org",
                  "replacementString": "http://localhost:3000/dbpedia.org"
                },
                {
                  "@type": "QuadTransformerReplaceIri",
                  "searchRegex": "^http://www.w3.org/2002/07/owl",
                  "replacementString": "http://localhost:3000/www.w3.org/2002/07/owl"
                }
              ],
              "fragmentationStrategy": {
                "@type": "FragmentationStrategyComposite",
                "strategies": [
                  { "@type": "FragmentationStrategySubject" }
                ]
              },
              "quadSink": {
              "@id": "urn:rdf-dataset-fragmenter:sink:default",
              "@type": "QuadSinkFile",
              "log": true,
              "outputFormat": "application/n-quads",
              "fileExtension": ".nq",
                "iriToPath": {
                  "http://": "out-fragments/http/",
                  "https://": "out-fragments/https/"
                }
              }
            }
            
            `);
        });

        expect(() => generator.getShapeTreeGeneratorInformation()).toThrow();
      });

      it('should walk into solid pods given getShapeTreeGeneratorInformation return valid information', async() => {
        // Make getShapeTreeGeneratorInformation return information
        jest.spyOn(generator, 'getFragmentConfig').mockImplementation(() => {
          return JSON.parse(`
          {
            "transformers": [
              {
                "@type": "QuadTransformerBlankToNamed",
                "searchRegex": "^(b[0-9]*_tagclass)",
                "replacementString": "http://localhost:3000/www.ldbc.eu/tagclass/$1"
              },
              {
                "@type": "QuadTransformerReplaceIri",
                "searchRegex": "^http://www.ldbc.eu",
                "replacementString": "http://localhost:3000/www.ldbc.eu"
              }
            ],
            "quadSink": {
              "@id": "urn:rdf-dataset-fragmenter:sink:default",
                "@type": "QuadSinkComposite",
                "sinks": [
                  {
                    "@type": "QuadSinkFile",
                    "log": true,
                    "outputFormat": "application/n-quads",
                    "fileExtension": ".nq",
                    "iriToPath": {
                      "http://": "out-fragments/http/",
                      "https://": "out-fragments/https/"
                    }
                  },
                  {
                    "@type": "QuadSinkFiltered",
                    "filter": {
                      "@type": "QuadMatcherResourceType",
                      "typeRegex": "vocabulary/Person$",
                      "matchFullResource": false
                    },
                    "sink": {
                      "@type": "QuadSinkCsv",
                      "file": "out-fragments/parameters-persons.csv",
                      "columns": [
                        "subject"
                      ]
                    }
                  },
                  {
                    "@type": "QuadSinkFiltered",
                    "filter": {
                      "@type": "QuadMatcherResourceType",
                      "typeRegex": "vocabulary/Comment$",
                      "matchFullResource": false
                    },
                    "sink": {
                      "@type": "QuadSinkCsv",
                      "file": "out-fragments/parameters-comments.csv",
                      "columns": [
                        "subject"
                      ]
                    }
                  },
                  {
                    "@type": "QuadSinkFiltered",
                    "filter": {
                      "@type": "QuadMatcherResourceType",
                      "typeRegex": "vocabulary/Post$",
                      "matchFullResource": false
                    },
                    "sink": {
                      "@type": "QuadSinkCsv",
                      "file": "out-fragments/parameters-posts.csv",
                      "columns": [
                        "subject"
                      ]
                    }
                  }
                ]
              }
          }
          `);
        });
        await generator.generateShapeTree();
        expect(walkSolidPods).toHaveBeenCalledTimes(1);
      });
    });
  });

  describe('generate', () => {
    describe('when overwrite is enabled', () => {
      it('should run all phases if directories do not exist yet', async() => {
        await generator.generate();

        expect(container.start).toHaveBeenCalled();
        expect(runEnhancer).toHaveBeenCalledWith('enhancementConfig', { mainModulePath });
        expect(runFragmenter).toHaveBeenCalledWith('fragmentConfig', { mainModulePath });
        expect(runFragmenter).toHaveBeenCalledWith('enhancementFragmentConfig', { mainModulePath });
        expect(runQueryInstantiator)
          .toHaveBeenCalledWith('queryConfig', { mainModulePath }, { variables: expect.anything() });
        expect(runValidationGenerator)
          .toHaveBeenCalledWith('validationConfig', { mainModulePath }, { variables: expect.anything() });
      });

      it('should skip phases with existing directories', async() => {
        files[Path.join('CWD', 'out-snb')] = 'a';
        files[Path.join('CWD', 'out-enhanced')] = 'a';
        files[Path.join('CWD', 'out-fragments')] = 'a';
        files[Path.join('CWD', 'out-queries')] = 'a';
        files[Path.join('CWD', 'out-validate')] = 'a';

        await generator.generate();

        expect(container.start).toHaveBeenCalled();
        expect(runEnhancer).toHaveBeenCalledWith('enhancementConfig', { mainModulePath });
        expect(runFragmenter).toHaveBeenCalledWith('fragmentConfig', { mainModulePath });
        expect(runFragmenter).toHaveBeenCalledWith('enhancementFragmentConfig', { mainModulePath });
        expect(runQueryInstantiator)
          .toHaveBeenCalledWith('queryConfig', { mainModulePath }, { variables: expect.anything() });
        expect(runValidationGenerator)
          .toHaveBeenCalledWith('validationConfig', { mainModulePath }, { variables: expect.anything() });
      });
    });

    describe('when overwrite is disabled', () => {
      beforeEach(() => {
        generator = new Generator({
          cwd: 'CWD',
          verbose: true,
          overwrite: false,
          scale: '0.1',
          enhancementConfig: 'enhancementConfig',
          fragmentConfig: 'fragmentConfig',
          enhancementFragmentConfig: 'enhancementFragmentConfig',
          queryConfig: 'queryConfig',
          validationParams: 'validationParams',
          validationConfig: 'validationConfig',
          hadoopMemory: '4G',
        });
      });

      it('should run all phases if directories do not exist yet', async() => {
        await generator.generate();

        expect(container.start).toHaveBeenCalled();
        expect(runEnhancer).toHaveBeenCalledWith('enhancementConfig', { mainModulePath });
        expect(runFragmenter).toHaveBeenCalledWith('fragmentConfig', { mainModulePath });
        expect(runFragmenter).toHaveBeenCalledWith('enhancementFragmentConfig', { mainModulePath });
        expect(runQueryInstantiator)
          .toHaveBeenCalledWith('queryConfig', { mainModulePath }, { variables: expect.anything() });
        expect(runValidationGenerator)
          .toHaveBeenCalledWith('validationConfig', { mainModulePath }, { variables: expect.anything() });
      });

      it('should skip phases with existing directories', async() => {
        files[Path.join('CWD', 'out-snb')] = 'a';
        files[Path.join('CWD', 'out-enhanced')] = 'a';
        files[Path.join('CWD', 'out-fragments')] = 'a';
        files[Path.join('CWD', 'out-queries')] = 'a';
        files[Path.join('CWD', 'out-validate')] = 'a';

        await generator.generate();

        expect(container.start).not.toHaveBeenCalled();
        expect(runEnhancer).not.toHaveBeenCalled();
        expect(runFragmenter).not.toHaveBeenCalled();
        expect(runFragmenter).not.toHaveBeenCalled();
        expect(runQueryInstantiator).not.toHaveBeenCalled();
        expect(runValidationGenerator).not.toHaveBeenCalled();
      });
    });

    it('Should not run generateShapeTree when generateShapeTree is false', async() => {
      generator = new Generator({
        cwd: 'CWD',
        verbose: true,
        overwrite: false,
        scale: '0.1',
        enhancementConfig: 'enhancementConfig',
        fragmentConfig: 'fragmentConfig',
        enhancementFragmentConfig: 'enhancementFragmentConfig',
        queryConfig: 'queryConfig',
        validationParams: 'validationParams',
        validationConfig: 'validationConfig',
        hadoopMemory: '4G',
        generateShapeTree: false,
      });
      const spyGenerateShapeTree = jest.spyOn(generator, 'generateShapeTree');
      await generator.generate();

      expect(spyGenerateShapeTree).not.toHaveBeenCalled();

      spyGenerateShapeTree.mockReset();
      spyGenerateShapeTree.mockRestore();
    });

    it('Should run generateShapeTree when generateShapeTree is true', async() => {
      generator = new Generator({
        cwd: 'CWD',
        verbose: true,
        overwrite: false,
        scale: '0.1',
        enhancementConfig: 'enhancementConfig',
        fragmentConfig: 'fragmentConfig',
        enhancementFragmentConfig: 'enhancementFragmentConfig',
        queryConfig: 'queryConfig',
        validationParams: 'validationParams',
        validationConfig: 'validationConfig',
        hadoopMemory: '4G',
        generateShapeTree: true,
      });
      const spyGenerateShapeTree = jest.spyOn(generator, 'generateShapeTree')
        .mockImplementation((): Promise<void> => { return new Promise(resolve => { resolve(); }); });

      await generator.generate();

      expect(spyGenerateShapeTree).toHaveBeenCalled();

      spyGenerateShapeTree.mockReset();
      spyGenerateShapeTree.mockRestore();
    });
  });
});
