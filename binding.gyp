{
  "targets": [
    {
      "target_name": "comparative_parquet",
      'defines': [ 'NAPI_DISABLE_CPP_EXCEPTIONS' ],
      "sources": [
        "src/main.cc",
      ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")"
      ],
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ],
      "cflags": [
          "<!@(pkg-config --cflags parquet arrow) -Wall -g",
      ],
      "ldflags": [
          "-Wl,-no-as-needed",
          "<!@(pkg-config --libs parquet arrow) -lpthread",
      ],
      "conditions": [
          ['OS == "linux"', {
              "defines": [
                  "PLATFORM_LINUX=1",
              ]
          }],
          ['OS == "mac"', {
              "defines": [
                  "PLATFORM_MAC=1",
              ],
              "xcode_settings": {
                  "OTHER_CFLAGS": [
                      "<!@(pkg-config --cflags parquet arrow)",
                  ],
                  "OTHER_LDFLAGS": [
                      "<!@(pkg-config --libs parquet arrow) -lpthread",
                  ]
              },
          }],
          ['OS == "win"', {
              "defines": [
                  "PLATFORM_WIN=1",
              ],
              "include_dirs": [
                  "include",
                  # "/msys64/mingw64/include/gobject-introspection-1.0",
                  # "/msys64/mingw64/lib/libffi-3.2.1/include",
              ]
          }]
      ]
    }
  ]
}
