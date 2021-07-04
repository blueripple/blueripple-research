{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module VA_DataPaths where

vaDataDir :: FilePath
vaDataDir = "./br-2021-VA/data/"

vaUpper2019CSV :: FilePath
vaUpper2019CSV =
  vaDataDir ++ "va_cvap_2019_sldu.csv"

vaLower2019CSV :: FilePath
vaLower2019CSV =
  vaDataDir ++ "va_cvap_2019_sldl.csv"
