{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module GA_DataPaths where

--import qualified Frames.TH                     as F

dataDir :: FilePath
dataDir = "../Georgia/data/"

electionDir :: FilePath
electionDir = dataDir <> "election/"

senate1CSV :: FilePath
senate1CSV =
 electionDir ++ "long/Senate1.csv"

senate2CSV :: FilePath
senate2CSV =
 electionDir ++ "long/Senate2.csv"
