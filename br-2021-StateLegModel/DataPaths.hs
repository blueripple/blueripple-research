{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module DataPaths where

sldDataDir :: FilePath
sldDataDir = "./br-2021-StateLegModel/data/"

vaUpper2019CSV :: FilePath
vaUpper2019CSV =
  sldDataDir ++ "va_cvap_2019_sldu.csv"

vaLower2019CSV :: FilePath
vaLower2019CSV =
  sldDataDir ++ "va_cvap_2019_sldl.csv"
