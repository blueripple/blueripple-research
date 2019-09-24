{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE QuasiQuotes               #-}
{-# LANGUAGE ScopedTypeVariables       #-}
module BlueRipple.Configuration where

import           Data.String.Here               ( i )
import qualified Data.Text as T

brReadMore :: T.Text
brReadMore = [i|
*Want to read more from Blue Ripple?
Visit our [website](${brHome}),
sign up for [email updates](${brEmailSignup}),
and follow us on [Twitter](${brTwitter})
and [FaceBook](${brFaceBook}).
Folks interested in our data and modeling efforts should also check out our
[Github](${brGithub}) page.*              
|]

brHome :: T.Text
brHome = "https://www.blueripplepolitics.org"

brEmailSignup :: T.Text
brEmailSignup = "http://eepurl.com/gzmeQ5"

brTwitter :: T.Text
brTwitter = "https://twitter.com/BlueRipplePol"

brFaceBook :: T.Text
brFaceBook = "https://www.facebook.com/blueripplepolitics"

brGithub :: T.Text
brGithub = "https://github.com/blueripple"

brGithubLanding :: T.Text
brGithubLanding = brGithub <> "/Guide"

brResearch :: T.Text
brResearch = "research"

brResearchRootUrl :: T.Text
brResearchRootUrl = "https://blueripple.github.io/" <> brResearch <> "/"

brResearchRootPath :: T.Text
brResearchRootPath = "/" <> brResearch <> "/"
